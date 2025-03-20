package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class DataUploader {

    private final S3Client s3Client;


    public DataUploader(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Uploads processed minute data to S3
     *
     * @param processedFile  Path to the processed minute data file
     * @param s3OutputBucket S3 bucket to upload to
     * @param s3Key The key for the data file
     * @return S3 path where data was uploaded
     * @throws IOException if there's an error during compression or upload
     */
    public String uploadMinuteData(Path processedFile, String s3OutputBucket, String s3Key) throws IOException {
        // Extract the original file name from the processed file path
        String originalFileName = processedFile.getFileName().toString();

        // Prepare output LZO file in temp directory
        Path tempDir = Files.createTempDirectory("s3upload");
        Path outputLzoFile = tempDir.resolve(originalFileName + ".lzo");

        log.info("Compressing {} to {}", processedFile, outputLzoFile);
        compressToLzo(processedFile, outputLzoFile);


        log.info("Uploading {} to s3://{}/{}", outputLzoFile, s3OutputBucket, s3Key);
        uploadToS3(outputLzoFile, s3Key, s3OutputBucket);

        // Clean up temp files
        Files.deleteIfExists(outputLzoFile);
        Files.deleteIfExists(tempDir);

        return s3Key;
    }

    private void compressToLzo(Path inputFile, Path outputLzoFile) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("lzop", "-o", outputLzoFile.toString(), inputFile.toString());
        Process process = processBuilder.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.debug("lzop output: {}", line);
            }
        }

        try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = errorReader.readLine()) != null) {
                log.warn("lzop error: {}", line);
            }
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("lzop compression failed with exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("lzop compression was interrupted", e);
        }
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

            log.info("Successfully uploaded to S3 with ETag: {}", response.eTag());
        } catch (Exception e) {
            throw new IOException("Failed to upload file to S3: " + e.getMessage(), e);
        }
    }
}