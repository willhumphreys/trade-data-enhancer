package uk.co.threebugs;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
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
    private final List<DataInterval> intervals = Arrays.asList(DataInterval.builder().name("1min").dirName("minute").s3Path("1min").build(), DataInterval.builder().name("1hour").dirName("hourly").s3Path("1hour").build(), DataInterval.builder().name("1day").dirName("daily").s3Path("1day").build());

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
            Path fixedDataFile = dataFile.getLocalPath().resolveSibling(dataFile.getLocalPath().getFileName() + "F.csv");
            new PolygonDataConverter().convert(dataFile.getLocalPath(), fixedDataFile);

            dataFiles.put(interval.getName(), new DataFileInfo(fixedDataFile, dataFile.getS3Path()));
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

        String key = s3_path + "/" + symbol + "_" + provider + "_" + interval.getS3Path() + ".csv.lzo";

        Path localLzoPath = Path.of(key.replace("/", "_"));
        Path localCsvPath = Path.of(key.replace("/", "_").replace(".csv.lzo", ".csv"));

        log.info("Fetching {} from S3: {}", interval.getName(), key);

        downloadFromS3(key, localLzoPath, inputBucketName);
        // Try up to 3 times to decompress
        decompressLzo(localLzoPath, localCsvPath);
        // Delete the LZO file after successful decompression
        Files.deleteIfExists(localLzoPath);
        return new FetchResult(localCsvPath, key);


    }

    private void downloadFromS3(String s3Path, Path localPath, String inputBucketName) {

        log.info("Downloading {} to {}", s3Path, localPath);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(inputBucketName).key(s3Path).build();

        s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(localPath.toFile()));
    }

    private void decompressLzo(Path lzoPath, Path csvPath) throws IOException {
        log.info("Decompressing {} to {}", lzoPath, csvPath);

        // Check if LZO file exists and has content
        if (!Files.exists(lzoPath) || Files.size(lzoPath) == 0) {
            log.error("LZO file {} doesn't exist or is empty", lzoPath);
            throw new IllegalArgumentException("LZO file doesn't exist or is empty: " + lzoPath);
        }

        // Use lzop command-line tool for decompression
        ProcessBuilder processBuilder = new ProcessBuilder("lzop", "-d", "-f", "-o", csvPath.toString(), lzoPath.toString());

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

    @Data
    @Builder
    static class DataInterval {
        private String name;
        private String dirName;
        private String s3Path;
    }
}