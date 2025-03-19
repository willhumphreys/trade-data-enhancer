package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
public class DataUploader {

    private final S3Client s3Client;
    private final String symbol;
    private final String provider;

    public DataUploader(String symbol, String provider, S3Client s3Client) {
        this.symbol = symbol;
        this.provider = provider;
        this.s3Client = s3Client;
    }

    /**
     * Uploads processed minute data to S3
     *
     * @param processedFile  Path to the processed minute data file
     * @param s3OutputBucket
     * @return S3 path where data was uploaded
     * @throws IOException if there's an error during compression or upload
     */
    public String uploadMinuteData(Path processedFile, String s3OutputBucket) throws IOException {
        if (!Files.exists(processedFile)) {
            throw new IOException("Processed file not found: " + processedFile);
        }

        // Create path for minute data
        String today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        String s3Directory = String.format("%s/%s/1min/%s/1min",
                provider, symbol, today);

        // Create temporary LZO file
        Path lzoFile = Files.createTempFile(processedFile.getFileName().toString(), ".lzo");
        compressToLzo(processedFile, lzoFile);

        // Upload to S3
        String s3Key = String.format("%s/%s.csv.lzo", s3Directory, symbol.toLowerCase());
        uploadToS3(lzoFile, s3Key, s3OutputBucket);

        // Clean up temporary file
        Files.delete(lzoFile);

        log.info("Uploaded minute data to S3: {}", s3Key);
        return s3Key;
    }

    private void compressToLzo(Path inputFile, Path outputLzoFile) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "lzop", "-c", inputFile.toString());

        Process process = processBuilder.start();

        // Redirect output to file
        try (InputStream is = process.getInputStream()) {
            Files.copy(is, outputLzoFile);
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                    String line;
                    StringBuilder error = new StringBuilder();
                    while ((line = reader.readLine()) != null) {
                        error.append(line).append("\n");
                    }
                    throw new IOException("Failed to compress file: " + error);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Compression process interrupted", e);
        }

        log.debug("Compressed {} to {}", inputFile, outputLzoFile);
    }

    private void uploadToS3(Path file, String s3Key, String s3OutputBucket) throws IOException {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(s3OutputBucket)
                    .key(s3Key)
                    .build();

            PutObjectResponse response = s3Client.putObject(
                    request,
                    RequestBody.fromFile(file)
            );

            log.debug("Successfully uploaded to S3: s3://{}/{} (ETag: {})",
                    s3OutputBucket, s3Key, response.eTag());
        } catch (Exception e) {
            throw new IOException("Failed to upload to S3: " + e.getMessage(), e);
        }
    }
}