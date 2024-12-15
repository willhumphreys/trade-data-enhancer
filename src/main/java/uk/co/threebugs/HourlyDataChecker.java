package uk.co.threebugs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class HourlyDataChecker {

    /**
     * Processes the input file, ensuring there is an entry at the start of each hour. Missing entries
     * are created with values copied from the previous entry but with Volume set to 0.
     * <p>
     * This is done in a streaming fashion to avoid loading the entire dataset into memory.
     *
     * @param inputPath  Path to the input file.
     * @param outputPath Path to the output file.
     * @throws IOException if reading or writing the file fails.
     */
    public void ensureHourlyEntries(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            String header = reader.readLine(); // Read and write the CSV header
            if (header == null) throw new IOException("Input data file is empty.");
            writer.write(header);
            writer.newLine();

            String previousLine = null;
            LocalDateTime previousHourMarker = null;

            String line;
            while ((line = reader.readLine()) != null) {
                DataEntry currentEntry = parseDataEntry(line);
                LocalDateTime currentTimestamp = currentEntry.timestamp().withMinute(0).withSecond(0).withNano(0);

                if (previousHourMarker != null) {
                    LocalDateTime expectedHour = previousHourMarker.plusHours(1);

                    // Fill any gaps between the previous marker and the current line
                    while (expectedHour.isBefore(currentTimestamp)) {
                        DataEntry missingEntry = createMissingHourlyEntry(expectedHour, parseDataEntry(previousLine));
                        writer.write(formatDataEntry(missingEntry));
                        writer.newLine();
                        expectedHour = expectedHour.plusHours(1);
                    }
                }

                // Write the current line to the output
                writer.write(line);
                writer.newLine();
                previousLine = line;
                previousHourMarker = currentTimestamp;
            }
        }
    }

    /**
     * Parses a line from the CSV file to create a DataEntry object.
     */
    private DataEntry parseDataEntry(String line) {
        String[] parts = line.split(",");

        // Parse the timestamp from epoch time
        double epochSeconds = Double.parseDouble(parts[0]);
        LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochSecond((long) epochSeconds), ZoneOffset.UTC);

        // Parse other values: Open, High, Low, Close, Volume
        double open = Double.parseDouble(parts[1]);
        double high = Double.parseDouble(parts[2]);
        double low = Double.parseDouble(parts[3]);
        double close = Double.parseDouble(parts[4]);
        double volume = Double.parseDouble(parts[5]);

        // Create DataEntry using the builder
        return DataEntry.builder()
                .timestamp(timestamp)
                .open(open)
                .high(high)
                .low(low)
                .close(close)
                .volume(volume)
                .build();
    }

    /**
     * Formats a DataEntry object as a CSV line.
     */
    private String formatDataEntry(DataEntry entry) {
        return entry.timestamp().toEpochSecond(ZoneOffset.UTC) + "," +
                entry.open() + "," +
                entry.high() + "," +
                entry.low() + "," +
                entry.close() + "," +
                entry.volume();
    }

    /**
     * Creates a new DataEntry object to fill missing hours.
     *
     * @param timestamp     The timestamp for the missing hour.
     * @param previousEntry The previous entry to copy values from.
     * @return A new DataEntry with values copied for Open/High/Low/Close and Volume set to 0.
     */
    private DataEntry createMissingHourlyEntry(LocalDateTime timestamp, DataEntry previousEntry) {
        return DataEntry.builder()
                .timestamp(timestamp)
                .open(previousEntry.open())
                .high(previousEntry.high())
                .low(previousEntry.low())
                .close(previousEntry.close())
                .volume(0.0) // Set Volume to 0 for the missing entry
                .build();
    }
}