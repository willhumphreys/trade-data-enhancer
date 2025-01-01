package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Utility class for generating hourly data from minute data.
 */
@Slf4j
public class HourlyFileGenerator {

    /**
     * Generates the hourly data file from the minute data file.
     *
     * @param minuteDataPath Path to the minute data file.
     * @param hourlyDataPath Path to the output hourly data file.
     * @throws IOException If there is an error reading or writing files.
     */
    public void generateHourlyFileFromMinuteFile(Path minuteDataPath, Path hourlyDataPath) throws IOException {
        log.info("Generating hourly data from minute data...");

        // Parse minute data and aggregate into hourly data.
        List<HourlyData> hourlyDataList = aggregateHourlyData(minuteDataPath);

        // Write the aggregated hourly data to the hourly output file.
        try (BufferedWriter writer = Files.newBufferedWriter(hourlyDataPath)) {
            writer.write("Timestamp,Open,High,Low,Close,Volume"); // Write the header
            writer.newLine();

            for (HourlyData hourlyData : hourlyDataList) {
                writer.write(hourlyData.toCsvRow());
                writer.newLine();
            }
        }

        log.info("Hourly data generated at '{}'.", hourlyDataPath);
    }

    /**
     * Aggregates minute data into hourly data.
     *
     * @param minuteDataPath Path of the minute data file.
     * @return A list of aggregated hourly data rows.
     * @throws IOException If there is an error reading from the file.
     */
    private List<HourlyData> aggregateHourlyData(Path minuteDataPath) throws IOException {
        Map<Long, List<MinuteTick>> hourlyBuckets = new HashMap<>(); // Use UNIX timestamp for grouping

        // Read minute data and populate hourly buckets
        try (BufferedReader reader = Files.newBufferedReader(minuteDataPath)) {
            String line = reader.readLine(); // Read header
            if (line == null || !line.matches(".*Timestamp.*")) {
                throw new IllegalArgumentException("Invalid or empty file. Expected 'Timestamp' in the header.");
            }

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 6) continue; // Skip invalid rows

                // Parse data from the input row
                long unixTimestamp = (long) Double.parseDouble(parts[0].trim());
                double open = Double.parseDouble(parts[1].trim());
                double high = Double.parseDouble(parts[2].trim());
                double low = Double.parseDouble(parts[3].trim());
                double close = Double.parseDouble(parts[4].trim());
                double volume = Double.parseDouble(parts[5].trim());

                // Compute the hour bucket (truncating to nearest hour)
                long hourTimestamp = unixTimestamp - (unixTimestamp % 3600);

                // Add the tick to the appropriate bucket
                hourlyBuckets.computeIfAbsent(hourTimestamp, _ -> new ArrayList<>())
                        .add(new MinuteTick(unixTimestamp, open, high, low, close, volume));
            }
        }

        // Aggregate each hourly bucket into a single hourly row
        List<HourlyData> hourlyDataList = new ArrayList<>();
        for (Map.Entry<Long, List<MinuteTick>> entry : hourlyBuckets.entrySet()) {
            long hourTimestamp = entry.getKey();
            List<MinuteTick> ticks = entry.getValue();

            // Aggregate open, high, low, close, and volume
            double open = ticks.getFirst().open;
            double close = ticks.getLast().close;
            double high = ticks.stream().mapToDouble(t -> t.high).max().orElseThrow();
            double low = ticks.stream().mapToDouble(t -> t.low).min().orElseThrow();
            double volume = ticks.stream().mapToDouble(t -> t.volume).sum();

            // Add the aggregated data as an HourlyData instance
            hourlyDataList.add(new HourlyData(hourTimestamp, open, high, low, close, volume));
        }

        // Ensure the hourly list is sorted by timestamp
        hourlyDataList.sort(Comparator.comparingLong(data -> data.timestamp));

        return hourlyDataList;
    }
}

/**
 * Represents a single tick of minute-level data.
 */
class MinuteTick {
    final long timestamp; // Unix timestamp in seconds
    final double open;
    final double high;
    final double low;
    final double close;
    final double volume;

    public MinuteTick(long timestamp, double open, double high, double low, double close, double volume) {
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }
}

/**
 * Represents a single row of hourly data with OHLC (Open, High, Low, Close) and volume values.
 */
class HourlyData {
    final long timestamp; // Unix timestamp in seconds
    final double open;
    final double high;
    final double low;
    final double close;
    final double volume;

    public HourlyData(long timestamp, double open, double high, double low, double close, double volume) {
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    /**
     * Converts this HourlyData instance to a CSV-formatted row.
     *
     * @return CSV row string.
     */
    public String toCsvRow() {
        return String.format("%.1f,%.2f,%.2f,%.2f,%.2f,%.2f",
                (double) timestamp, open, high, low, close, volume);
    }
}