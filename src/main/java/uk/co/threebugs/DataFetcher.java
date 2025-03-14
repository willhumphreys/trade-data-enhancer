package uk.co.threebugs;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoLibrary;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

@Slf4j
public class DataFetcher {

    private static final String S3_BUCKET = "mochi-tickdata-historical";
    private final S3Client s3Client;
    private final String symbol;
    private final String provider;
    private final Path dataDir;
    private final List<DataInterval> intervals = Arrays.asList(DataInterval.builder().name("1min").dirName("minute").s3Path("1min").build(), DataInterval.builder().name("1hour").dirName("hourly").s3Path("1hour").build(), DataInterval.builder().name("1day").dirName("daily").s3Path("1day").build());

    public DataFetcher(String symbol, String provider, Path dataDir) {
        this.symbol = symbol != null ? symbol : "AAPL";
        this.provider = provider != null ? provider : "polygon";
        this.dataDir = dataDir;
        this.s3Client = S3Client.builder().region(Region.US_EAST_1) // Adjust if your bucket is in a different region
                .build();

        // Create data directories if they don't exist
        for (DataInterval interval : intervals) {
            try {
                Path dir = dataDir.resolve(interval.getDirName());
                Files.createDirectories(dir);
            } catch (IOException e) {
                log.error("Failed to create data directory for {}", interval.getDirName(), e);
            }
        }
    }

    public Map<String, Path> fetchData() throws IOException {
        Map<String, Path> dataFiles = new HashMap<>();

        for (DataInterval interval : intervals) {
            Path dataFile = fetchIntervalData(interval);
            if (dataFile != null) {
                dataFiles.put(interval.getName(), dataFile);
            } else {
                log.error("Failed to fetch data for interval {}", interval.getName());
                return Collections.emptyMap(); // Abort if any data is missing
            }
        }

        return dataFiles;
    }

    private Path fetchIntervalData(DataInterval interval) throws IOException {
        Path targetDir = dataDir.resolve(interval.getDirName());

        // Check if we already have CSV files
        try (var files = Files.newDirectoryStream(targetDir, "*.csv")) {
            Optional<Path> existingFile = files.iterator().hasNext() ? Optional.of(files.iterator().next()) : Optional.empty();

            if (existingFile.isPresent()) {
                log.info("Using existing data file for {}: {}", interval.getName(), existingFile.get().getFileName());
                return existingFile.get();
            }
        }

        // Find the latest file in S3
        String s3Path = findLatestS3Path(interval);
        if (s3Path == null) {
            log.error("No data found in S3 for {} {}", symbol, interval.getName());
            return null;
        }

        // Download and decompress
        String filename = s3Path.substring(s3Path.lastIndexOf('/') + 1);
        Path localLzoPath = targetDir.resolve(filename);
        Path localCsvPath = targetDir.resolve(filename.replace(".lzo", ".csv"));

        if (downloadFromS3(s3Path, localLzoPath)) {
            if (decompressLzo(localLzoPath, localCsvPath)) {
                // Delete the LZO file after successful decompression
                Files.deleteIfExists(localLzoPath);
                return localCsvPath;
            }
        }

        return null;
    }

    private String findLatestS3Path(DataInterval interval) {
        String prefix = String.format("stocks/%s/%s/%s/", symbol, provider, interval.getS3Path());

        try {
            // Find latest year
            String latestYear = findLatestDirectory(prefix);
            if (latestYear == null) return null;

            // Find latest month
            String monthPrefix = prefix + latestYear + "/";
            String latestMonth = findLatestDirectory(monthPrefix);
            if (latestMonth == null) return null;

            // Find latest day
            String dayPrefix = monthPrefix + latestMonth + "/";
            String latestDay = findLatestDirectory(dayPrefix);
            if (latestDay == null) return null;

            if ("1min".equals(interval.getS3Path())) {
                // For minute data, we need to find latest hour too
                String hourPrefix = dayPrefix + latestDay + "/";
                String latestHour = findLatestDirectory(hourPrefix);
                if (latestHour == null) return null;

                // Find latest file
                String filePrefix = hourPrefix + latestHour + "/";
                return findLatestFile(filePrefix, ".lzo");
            } else {
                // For hourly/daily just find latest file
                String filePrefix = dayPrefix + latestDay + "/";
                return findLatestFile(filePrefix, ".lzo");
            }
        } catch (Exception e) {
            log.error("Error finding latest S3 path for {}", interval.getName(), e);
            return null;
        }
    }

    private String findLatestDirectory(String prefix) {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(S3_BUCKET).prefix(prefix).delimiter("/").build();

            ListObjectsV2Response response = s3Client.listObjectsV2(request);

            return response.commonPrefixes().stream().map(CommonPrefix::prefix).map(p -> p.replace(prefix, "").replace("/", "")).max(Comparator.naturalOrder()).orElse(null);
        } catch (Exception e) {
            log.error("Error listing directories at {}", prefix, e);
            return null;
        }
    }

    private String findLatestFile(String prefix, String suffix) {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(S3_BUCKET).prefix(prefix).build();

            ListObjectsV2Response response = s3Client.listObjectsV2(request);

            return response.contents().stream().filter(s3Object -> s3Object.key().endsWith(suffix)).max(Comparator.comparing(S3Object::lastModified)).map(S3Object::key).orElse(null);
        } catch (Exception e) {
            log.error("Error finding latest file at {}", prefix, e);
            return null;
        }
    }

    private boolean downloadFromS3(String s3Path, Path localPath) {
        try {
            log.info("Downloading {} to {}", s3Path, localPath);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(S3_BUCKET).key(s3Path).build();

            s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(localPath.toFile()));
            return true;
        } catch (Exception e) {
            log.error("Failed to download {}", s3Path, e);
            return false;
        }
    }

    private boolean decompressLzo(Path lzoPath, Path csvPath) {
        log.info("Decompressing {} to {}", lzoPath, csvPath);

        LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(LzoAlgorithm.LZO1X, null);

        try (InputStream is = new BufferedInputStream(Files.newInputStream(lzoPath)); InputStream decompressedStream = new LzoInputStream(is, decompressor); OutputStream os = new BufferedOutputStream(Files.newOutputStream(csvPath, StandardOpenOption.CREATE))) {

            // Use a simple copy operation with the decompressed stream
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = decompressedStream.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }

            return true;
        } catch (Exception e) {
            log.error("Failed to decompress {}", lzoPath, e);
            return false;
        }
    }

    @Data
    @Builder
    static class DataInterval {
        private String name;
        private String dirName;
        private String s3Path;
    }
}