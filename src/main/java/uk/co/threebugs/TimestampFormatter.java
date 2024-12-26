package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * TimestampFormatter processes a file to modify the timestamp format
 * from Unix Epoch (e.g., 1325412060) to ISO-8601 (e.g., 2012-01-01T10).
 */
@Slf4j
public class TimestampFormatter {

    private static final DateTimeFormatter TARGET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm").withZone(ZoneOffset.UTC);

    /**
     * Reads the input file, reformats the timestamp column, and writes to an output file.
     *
     * @param inputPath  Path to the input file.
     * @param outputPath Path to the output file.
     */
    public void reformatTimestamps(Path inputPath, Path outputPath) {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            log.info("Reading input file at: {}", inputPath);

            // Process the header row
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("The input file is empty.");
            }

            writer.write(header); // Write the header row as is
            writer.newLine();

            // Process data rows
            int processedCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String formattedLine = reformatRow(line);
                writer.write(formattedLine);
                writer.newLine();
                processedCount++;
            }

            log.info("Processed {} rows and reformatted timestamp.", processedCount);
        } catch (IOException e) {
            log.error("Error processing file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Reformats a single row by converting the 'Timestamp' column.
     *
     * @param row A CSV row as a string.
     * @return A new CSV row with the formatted timestamp.
     */
    private String reformatRow(String row) {
        String[] columns = row.split(",");

        if (columns.length == 0) {
            log.warn("Skipping empty or invalid row: {}", row);
            return row; // Return the original row as-is
        }

        try {
            // Parse the timestamp (assuming the first column is "Timestamp")
            long unixTimestamp = Long.parseLong(columns[0]);
            String formattedTimestamp = TARGET_FORMATTER.format(Instant.ofEpochSecond(unixTimestamp));
            columns[0] = formattedTimestamp; // Replace the timestamp column

            // Rebuild the row
            return String.join(",", columns);

        } catch (NumberFormatException e) {
            log.error("Invalid timestamp in row: {}", row);
            return row; // Return the original row if timestamp is invalid
        }
    }
}