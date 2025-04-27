package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        return hoursInsertedCounter;
    }

    // Parses rows from CSV lines into RowData objects
    private List<RowData> parseRows(List<String> lines) {
        return lines.stream().map(line -> {
            String[] parts = line.split(",");
            // Assuming timestamp is the first column and volume is the last
            return new RowData(
                    Double.parseDouble(parts[0]), // Timestamp
                    Long.parseLong(parts[1]),     // Open
                    Long.parseLong(parts[2]),     // High
                    Long.parseLong(parts[3]),     // Low
                    Long.parseLong(parts[4]),     // Close
                    Double.parseDouble(parts[5])  // Volume
            );
        }).collect(Collectors.toList());
    }

    // Parses hourly rows into a map keyed by timestamp
    private Map<Long, RowData> parseHourlyRows(List<String> lines) {
        Map<Long, RowData> hourlyMap = new HashMap<>();
        for (String line : lines) {
            String[] parts = line.split(",");
            long timestamp = (long) Double.parseDouble(parts[0]); // Convert to long for key
            RowData row = new RowData(
                    timestamp,                    // Use the long timestamp
                    Long.parseLong(parts[1]),     // Open
                    Long.parseLong(parts[2]),     // High
                    Long.parseLong(parts[3]),     // Low
                    Long.parseLong(parts[4]),     // Close
                    Double.parseDouble(parts[5])  // Volume
            );
            hourlyMap.put(timestamp, row);
        }
        return hourlyMap;
    }

    // Aligns a timestamp (double) to the start of its hour (long)
    private long alignToStartOfHour(double timestamp) {
        return ((long) timestamp / 3600) * 3600;
    }

    // Checks if two timestamps fall into different hours
    private boolean isNewHour(double timestamp1, double timestamp2) {
        return alignToStartOfHour(timestamp1) != alignToStartOfHour(timestamp2);
    }

    // Checks if a timestamp is exactly at the start of an hour
    private boolean isAlignedToHour(double timestamp) {
        return timestamp % 3600 == 0;
    }

    // Checks if there's a gap of one or more full hours between two timestamps
    private boolean hasHourGap(double timestamp1, double timestamp2) {
        return alignToStartOfHour(timestamp2) > alignToStartOfHour(timestamp1) + 3600;
    }

    // Inner record to hold row data
    private record RowData(double timestamp, long open, long high, long low, long close, double volume) {
        // Format the row data into a CSV line, ensuring timestamp is a long integer string
        public String toCsvLine() {
            // Format timestamp as long (%d) to remove the decimal part
            return String.format("%d,%d,%d,%d,%d,%.6f",
                    (long) timestamp, open, high, low, close, volume);
        }
    }
}