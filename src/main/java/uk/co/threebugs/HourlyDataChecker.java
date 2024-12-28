package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class HourlyDataChecker {

    public void ensureHourlyEntries(Path inputPath, Path outputPath) throws IOException {
        // Read all lines from the input CSV file
        List<String> lines = Files.readAllLines(inputPath);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Input file is empty!");
        }

        // The first line is assumed to be the header
        String header = lines.get(0);
        List<String> outputLines = new ArrayList<>();
        outputLines.add(header); // Add header to output

        // Parse the data
        List<RowData> parsedData = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] columns = lines.get(i).split(",");
            long timestamp = (long) Double.parseDouble(columns[0]);
            parsedData.add(new RowData(
                    timestamp,
                    Long.parseLong(columns[1]), // Open
                    Long.parseLong(columns[2]), // High
                    Long.parseLong(columns[3]), // Low
                    Long.parseLong(columns[4]), // Close
                    Double.parseDouble(columns[5]) // Volume
            ));
        }

        // Create output data, filling gaps
        List<RowData> outputData = new ArrayList<>();
        RowData previousRow = null;
        for (RowData currentRow : parsedData) {
            if (previousRow != null) {
                // Fill missing hourly rows
                while (previousRow.timestamp + 3600 < currentRow.timestamp) {
                    previousRow = previousRow.generateNextHour();
                    outputData.add(previousRow);
                }
            }
            outputData.add(currentRow); // Add the current row
            previousRow = currentRow;
        }

        // Generate output lines
        for (RowData row : outputData) {
            outputLines.add(row.toCsvLine());
        }

        // Write the output back to the file
        Files.write(outputPath, outputLines);
    }

    /**
     * Helper class to represent a row of data.
     */
    private static class RowData {
        long timestamp; // Epoch time in seconds
        long open;
        long high;
        long low;
        long close;
        double volume;

        RowData(long timestamp, long open, long high, long low, long close, double volume) {
            this.timestamp = timestamp;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }

        RowData generateNextHour() {
            // Align to the start of the next hour
            long nextTimestamp = alignToStartOfHour(timestamp) + 3600;
            return new RowData(nextTimestamp, open, high, low, close, 0.0); // Default volume is set to 0.0
        }

        /**
         * Helper method to align a timestamp to the start of the hour.
         */
        private long alignToStartOfHour(long inputTimestamp) {
            return (inputTimestamp / 3600) * 3600;
        }

        /**
         * Convert a row to a CSV-formatted string.
         */
        String toCsvLine() {
            return String.format("%d.0,%d,%d,%d,%d,%f",
                    this.timestamp, this.open, this.high, this.low, this.close, this.volume);
        }
    }
}