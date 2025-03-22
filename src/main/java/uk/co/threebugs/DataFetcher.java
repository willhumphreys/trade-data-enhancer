package uk.co.threebugs;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import uk.co.threebugs.preconvert.PolygonDataConverter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Slf4j
public class DataFetcher {

    private final S3Client s3Client;
    private final String symbol;
    private final String provider;
    private final Path dataDir;
    private final List<DataInterval> intervals = Arrays.asList(
            DataInterval.builder().name("1min").dirName("minute").s3Path("1min").build(),
            DataInterval.builder().name("1hour").dirName("hourly").s3Path("1hour").build(),
            DataInterval.builder().name("1day").dirName("daily").s3Path("1day").build()
    );

    public DataFetcher(String symbol, String provider, Path dataDir, S3Client s3Client) {
        this.symbol = symbol != null ? symbol : "AAPL";
        this.provider = provider != null ? provider : "polygon";
        this.dataDir = dataDir;
        this.s3Client = s3Client;

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

    public Map<String, DataFileInfo> fetchData(String inputBucketName, String s3_path) throws IOException {
        Map<String, DataFileInfo> dataFiles = new HashMap<>();

        for (DataInterval interval : intervals) {
            FetchResult dataFile = fetchIntervalData(interval, inputBucketName, s3_path);
            if (dataFile != null) {

                Path fixedDataFile = dataFile.getLocalPath().resolveSibling(dataFile.getLocalPath().getFileName() + "F.csv");
                new PolygonDataConverter().convert(dataFile.getLocalPath(), fixedDataFile);

                dataFiles.put(interval.getName(), new DataFileInfo(fixedDataFile, dataFile.getS3Path()));
            } else {
                log.error("Failed to fetch data for interval {}", interval.getName());
                throw new IOException("Failed to fetch data for interval " + interval.getName());
            }
        }

        return dataFiles;
    }

    private FetchResult fetchIntervalData(DataInterval interval, String inputBucketName, String s3_path) throws IOException {
        Path targetDir = dataDir.resolve(interval.getDirName());

        // Check if we already have CSV files
        try (var files = Files.newDirectoryStream(targetDir, "*.csv.lzo")) {
            Iterator<Path> iterator = files.iterator();
            if (iterator.hasNext()) {
                Path existingFile = iterator.next();
                log.info("Using existing data file for {}: {}", interval.getName(), existingFile.getFileName());
                // For existing files, we don't have the S3 path, so return null for it
                return new FetchResult(existingFile, null);
            }
        }
        // Find the latest file in S3
        //String s3Path = findLatestS3Path(interval, inputBucketName);
        String s3Path = s3_path;
        if (s3Path == null) {
            log.error("No data found in S3 for {} {}", symbol, interval.getName());
            return null;
        }

        // Download and decompress
        String filename = s3Path.substring(s3Path.lastIndexOf('/') + 1);
        Path localLzoPath = targetDir.resolve(filename);
        Path localCsvPath = targetDir.resolve(filename.replace(".lzo", ""));


        if (downloadFromS3(s3Path, localLzoPath, inputBucketName)) {
            // Try up to 3 times to decompress
            decompressLzo(localLzoPath, localCsvPath);
            // Delete the LZO file after successful decompression
            Files.deleteIfExists(localLzoPath);
            return new FetchResult(localCsvPath, s3Path);
        }

        return null;
    }

    // Class to hold both the local path and S3 path
    @lombok.Value
    public static class DataFileInfo {
        Path localPath;
        String s3Path;
    }

    @lombok.Value
    private static class FetchResult {
        Path localPath;
        String s3Path;
    }

    private String findLatestS3Path(DataInterval interval, String inputBucketName) {
        String prefix = String.format("stocks/%s/%s/%s/", symbol, provider, interval.getS3Path());

        try {
            // Find latest year
            String latestYear = findLatestDirectory(prefix, inputBucketName);
            if (latestYear == null) return null;

            // Find latest month
            String monthPrefix = prefix + latestYear + "/";
            String latestMonth = findLatestDirectory(monthPrefix, inputBucketName);
            if (latestMonth == null) return null;

            // Find latest day
            String dayPrefix = monthPrefix + latestMonth + "/";
            String latestDay = findLatestDirectory(dayPrefix, inputBucketName);
            if (latestDay == null) return null;

            if ("1min".equals(interval.getS3Path())) {
                // For minute data, we need to find latest hour too
                String hourPrefix = dayPrefix + latestDay + "/";
                String latestHour = findLatestDirectory(hourPrefix, inputBucketName);
                if (latestHour == null) return null;

                // Find latest file
                String filePrefix = hourPrefix + latestHour + "/";
                return findLatestFile(filePrefix, ".lzo", inputBucketName);
            } else {
                // For hourly/daily just find latest file
                String filePrefix = dayPrefix + latestDay + "/";
                return findLatestFile(filePrefix, ".lzo", inputBucketName);
            }
        } catch (Exception e) {
            log.error("Error finding latest S3 path for {}", interval.getName(), e);
            return null;
        }
    }

    private String findLatestDirectory(String prefix, String inputBucketName) {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(inputBucketName).prefix(prefix).delimiter("/").build();

            ListObjectsV2Response response = s3Client.listObjectsV2(request);

            return response.commonPrefixes().stream().map(CommonPrefix::prefix).map(p -> p.replace(prefix, "").replace("/", "")).max(Comparator.naturalOrder()).orElse(null);
        } catch (Exception e) {
            log.error("Error listing directories at {}", prefix, e);
            return null;
        }
    }

    private String findLatestFile(String prefix, String suffix, String inputBucketName) {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(inputBucketName).prefix(prefix).build();

            ListObjectsV2Response response = s3Client.listObjectsV2(request);

            return response.contents().stream().filter(s3Object -> s3Object.key().endsWith(suffix)).max(Comparator.comparing(S3Object::lastModified)).map(S3Object::key).orElse(null);
        } catch (Exception e) {
            log.error("Error finding latest file at {}", prefix, e);
            return null;
        }
    }

    private boolean downloadFromS3(String s3Path, Path localPath, String inputBucketName) {
        try {
            log.info("Downloading {} to {}", s3Path, localPath);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(inputBucketName).key(s3Path).build();

            s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(localPath.toFile()));
            return true;
        } catch (Exception e) {
            log.error("Failed to download {}", s3Path, e);
            return false;
        }
    }

    private void decompressLzo(Path lzoPath, Path csvPath) throws IOException {
        log.info("Decompressing {} to {}", lzoPath, csvPath);

        // Check if LZO file exists and has content
        if (!Files.exists(lzoPath) || Files.size(lzoPath) == 0) {
            log.error("LZO file {} doesn't exist or is empty", lzoPath);
            throw new IllegalArgumentException("LZO file doesn't exist or is empty: " + lzoPath);
        }

        // Use lzop command-line tool for decompression
        ProcessBuilder processBuilder = new ProcessBuilder(
                "lzop", "-d", "-f", "-o", csvPath.toString(), lzoPath.toString()
        );

        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        // Read output for logging purposes
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.debug("lzop output: {}", line);
            }
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                log.info("Successfully decompressed {} to {}", lzoPath, csvPath);
            } else {
                log.error("lzop process exited with code {}", exitCode);
                // Clean up any partial output file
                Files.deleteIfExists(csvPath);
                throw new IOException("lzop decompression failed with exit code: " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for lzop process", e);
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