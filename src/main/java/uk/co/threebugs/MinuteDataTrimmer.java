package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class MinuteDataTrimmer {

    /**
     * Trims minute data to end on the same date as the last entry in the hourly data.
     *
     * @param minuteDataPath Path to the minute data file.
     * @param hourlyDataPath Path to the hourly data file.
     * @param trimmedOutput  Path to write the trimmed minute data.
     * @throws IOException If there is an issue reading or writing the files.
     */
    public void trimMinuteData(Path minuteDataPath, Path hourlyDataPath, Path trimmedOutput) throws IOException {
        log.info("Trimming minute data to match the date of the last entry in the hourly data...");

        // Get the cutoff date from the last hourly data entry
        LocalDateTime cutoffDate = getLastTimestampFromHourlyData(hourlyDataPath);
        log.info("Cutoff date determined from hourly data: {}", cutoffDate.toLocalDate());

        long totalRows = 0;
        long trimmedRows = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(minuteDataPath.toFile()));
             BufferedWriter writer = new BufferedWriter(new FileWriter(trimmedOutput.toFile()))) {

            // Process the header and write it to the trimmed file
            String header = reader.readLine();
            if (header == null) throw new IOException("Minute data file is empty.");
            writer.write(header);
            writer.newLine();

            // Get the index of the 'Timestamp' column dynamically in case the order changes
            int timestampIndex = getIndex(header.split(","), "Timestamp");

            // Process the rows
            String line;
            while ((line = reader.readLine()) != null) {
                totalRows++;

                // Parse each row into fields
                String[] fields = line.split(",");

                // Convert timestamp (UNIX format) into LocalDateTime
                double unixTimestamp = Double.parseDouble(fields[timestampIndex]);
                LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochSecond((long) unixTimestamp), ZoneOffset.UTC);

                // Only include rows where the timestamp is on the same date or earlier than the cutoff date
                if (timestamp.toLocalDate().isAfter(cutoffDate.toLocalDate())) {
                    trimmedRows++;
                } else {
                    // Write the row to the output
                    writer.write(line);
                    writer.newLine();
                }
            }
        }

        // Log the total number of rows processed and trimmed
        log.info("Minute data processed: {} total rows, {} rows trimmed.", totalRows, trimmedRows);
    }

    /**
     * Gets the last timestamp from the hourly data file (formatted as UNIX timestamps).
     *
     * @param hourlyDataPath Path to the hourly data file.
     * @return The timestamp of the last hourly entry as a LocalDateTime.
     * @throws IOException If there is an issue reading the file.
     */
    private LocalDateTime getLastTimestampFromHourlyData(Path hourlyDataPath) throws IOException {
        LocalDateTime lastTimestamp = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(hourlyDataPath.toFile()))) {
            String header = reader.readLine(); // Read and skip the header
            if (header == null) throw new IOException("Hourly data file is empty.");

            // Get the index of the 'Timestamp' column dynamically
            int timestampIndex = getIndex(header.split(","), "Timestamp");

            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");

                // Parse the last timestamp in the file
                double unixTimestamp = Double.parseDouble(fields[timestampIndex]);
                lastTimestamp = LocalDateTime.ofInstant(Instant.ofEpochSecond((long) unixTimestamp), ZoneOffset.UTC);
            }
        }

        if (lastTimestamp == null) {
            throw new IOException("Hourly data contains no valid entries.");
        }

        return lastTimestamp;
    }

    /**
     * Finds the index of a column by its name in the header.
     *
     * @param headers    Array of header column names.
     * @param columnName Name of the column to find.
     * @return Index of the column.
     */
    private int getIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equals(columnName.trim())) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }
}