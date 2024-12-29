package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HourlyDataChecker {

    /**
     * Ensures hourly entries are present and adds missing hourly rows.
     *
     * @param minuteDataPath Path to the minute data CSV file.
     * @param hourlyDataPath Path to the hourly data CSV file.
     * @param outputPath     Path to save the output CSV file with ensured hourly data.
     * @return The number of hours inserted during the process.
     * @throws IOException If file read/write operations fail.
     */
    public int ensureHourlyEntries(Path minuteDataPath, Path hourlyDataPath, Path outputPath) throws IOException {
        // Read all lines from the minute CSV file
        List<String> minuteLines = Files.readAllLines(minuteDataPath);
        if (minuteLines.isEmpty()) {
            throw new IllegalArgumentException("Minute data input file is empty!");
        }

        // Read all lines from the hourly CSV file
        List<String> hourlyLines = Files.readAllLines(hourlyDataPath);
        if (hourlyLines.isEmpty()) {
            throw new IllegalArgumentException("Hourly data input file is empty!");
        }

        // Parse headers and data rows
        String header = minuteLines.getFirst();
        List<RowData> minuteData = parseRows(minuteLines.subList(1, minuteLines.size()));
        Map<Long, RowData> hourlyData = parseHourlyRows(hourlyLines.subList(1, hourlyLines.size())); // Hourly map

        List<RowData> outputData = new ArrayList<>();
        RowData previousRow = null;
        int hoursInsertedCounter = 0; // Counter for inserted hourly rows

        for (RowData currentRow : minuteData) {
            // Fill any gap in hours
            if (previousRow != null && hasHourGap(previousRow.timestamp, currentRow.timestamp)) {
                long currentHour = alignToStartOfHour(previousRow.timestamp);

                while (currentHour + 3600 < alignToStartOfHour(currentRow.timestamp)) {
                    currentHour += 3600; // Move to the next hour
                    RowData hourlyRow = hourlyData.get(currentHour); // Check hourly map

                    if (hourlyRow != null) {
                        // Insert a new row with timestamp/open from hourlyRow and other values from previousRow
                        RowData missingRow = new RowData(
                                hourlyRow.timestamp,       // Timestamp from hourly data
                                hourlyRow.open,            // Open from hourly data
                                previousRow.high,          // High from previous row
                                previousRow.low,           // Low from previous row
                                previousRow.close,         // Close from previous row
                                previousRow.volume         // Volume from previous row
                        );
                        outputData.add(missingRow);
                        hoursInsertedCounter++; // Increment counter
                    }
                }
            }

            // Check for unaligned ticks within a new hour
            if (previousRow != null && isNewHour(previousRow.timestamp, currentRow.timestamp)) {
                if (!isAlignedToHour(currentRow.timestamp)) {
                    // Insert the hourly record if present at the starting boundary of the currentRow hour
                    long hourStartTimestamp = alignToStartOfHour(currentRow.timestamp);
                    RowData hourlyRow = hourlyData.get(hourStartTimestamp);

                    if (hourlyRow != null) {
                        RowData unalignedRow = new RowData(
                                hourlyRow.timestamp,       // Timestamp from hourly data
                                hourlyRow.open,            // Open from hourly data
                                previousRow.high,          // High from previous row
                                previousRow.low,           // Low from previous row
                                previousRow.close,         // Close from previous row
                                previousRow.volume         // Volume from previous row
                        );
                        outputData.add(unalignedRow);
                        hoursInsertedCounter++; // Increment counter
                    }
                }
            }

            // Add the current row to the output
            outputData.add(currentRow);
            previousRow = currentRow;
        }

        // Write the output to the file
        List<String> outputLines = new ArrayList<>();
        outputLines.add(header); // Add the header first
        outputLines.addAll(outputData.stream().map(RowData::toCsvLine).toList());
        Files.write(outputPath, outputLines);

        return hoursInsertedCounter; // Return count of hours inserted
    }

    // existing methods (parseRows, parseHourlyRows, alignToStartOfHour, etc.) remain unchanged...

    /**
     * Helper method to parse rows from the minute or hourly data.
     */
    private List<RowData> parseRows(List<String> lines) {
        List<RowData> data = new ArrayList<>();
        for (String line : lines) {
            data.add(RowData.fromCsvLine(line));
        }
        return data;
    }

    /**
     * Helper method to parse rows from hourly data into a map (key: timestamp).
     */
    private Map<Long, RowData> parseHourlyRows(List<String> lines) {
        Map<Long, RowData> hourlyData = new HashMap<>();
        for (String line : lines) {
            RowData row = RowData.fromCsvLine(line);
            hourlyData.put(row.timestamp, row);
        }
        return hourlyData;
    }

    /**
     * Aligns a timestamp to the start of its hour.
     */
    private long alignToStartOfHour(long timestamp) {
        return (timestamp / 3600) * 3600;
    }

    /**
     * Checks if there is a gap of hours between previous and current timestamps.
     */
    private boolean hasHourGap(long previousTimestamp, long currentTimestamp) {
        return (alignToStartOfHour(currentTimestamp) - alignToStartOfHour(previousTimestamp)) > 3600;
    }

    /**
     * Checks if the current timestamp represents a transition to a new hour.
     */
    private boolean isNewHour(long previousTimestamp, long currentTimestamp) {
        return alignToStartOfHour(previousTimestamp) != alignToStartOfHour(currentTimestamp);
    }

    /**
     * Checks if a given timestamp is aligned to the start of an hour.
     */
    private boolean isAlignedToHour(long timestamp) {
        return (timestamp % 3600) == 0;
    }

    /**
     * Helper class to represent a row of data.
     */
    private static class RowData {
        long timestamp;
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

        static RowData fromCsvLine(String line) {
            String[] parts = line.split(",");
            return new RowData(
                    (long) Double.parseDouble(parts[0]),
                    Long.parseLong(parts[1]),
                    Long.parseLong(parts[2]),
                    Long.parseLong(parts[3]),
                    Long.parseLong(parts[4]),
                    Double.parseDouble(parts[5])
            );
        }

        String toCsvLine() {
            return String.format("%d.0,%d,%d,%d,%d,%f",
                    this.timestamp, this.open, this.high, this.low, this.close, this.volume);
        }
    }
}