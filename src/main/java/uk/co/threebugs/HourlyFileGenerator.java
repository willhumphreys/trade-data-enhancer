package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Slf4j
public class HourlyFileGenerator {

    /**
     * Generates an aggregated data file (hourly or daily) from the minute data file.
     *
     * @param minuteDataPath Path to the minute data file.
     * @param outputPath     Path to the output data file.
     * @param aggregationType Aggregation type: HOURLY or DAILY.
     * @throws IOException If there is an error reading or writing files.
     */
    public void generateFileFromMinuteFile(Path minuteDataPath, Path outputPath, AggregationType aggregationType) throws IOException {
        log.info("Generating {} data from minute data...", aggregationType);

        // Aggregate the minute data based on the specified aggregation type.
        List<HourlyData> aggregatedData = aggregateMinuteData(minuteDataPath, aggregationType);

        // Write the aggregated data to the output file.
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            writer.write("Timestamp,Open,High,Low,Close,Volume");
            writer.newLine();
            for (HourlyData data : aggregatedData) {
                writer.write(data.toCsvRow());
                writer.newLine();
            }
        }

        log.info("{} data generated at '{}'.", aggregationType, outputPath);
    }

    /**
     * Aggregates minute data into either hourly or daily data.
     *
     * @param minuteDataPath Path of the minute data file.
     * @param aggregationType Aggregation type: HOURLY or DAILY.
     * @return A list of aggregated data rows.
     * @throws IOException If there is an error reading from the file.
     */
    private List<HourlyData> aggregateMinuteData(Path minuteDataPath, AggregationType aggregationType) throws IOException {
        Map<Long, List<MinuteTick>> buckets = new HashMap<>();
        // Bucket size: 3600 seconds for hourly aggregation, 86400 seconds for daily aggregation.
        long bucketSize = aggregationType == AggregationType.HOURLY ? 3600L : 86400L;

        try (BufferedReader reader = Files.newBufferedReader(minuteDataPath)) {
            String header = reader.readLine(); // Read header
            if (header == null || !header.contains("Timestamp")) {
                throw new IllegalArgumentException("Invalid or empty file. Expected 'Timestamp' in the header.");
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 6) continue; // Skip invalid rows

                long timestamp = (long) Double.parseDouble(parts[0].trim());
                double open = Double.parseDouble(parts[1].trim());
                double high = Double.parseDouble(parts[2].trim());
                double low = Double.parseDouble(parts[3].trim());
                double close = Double.parseDouble(parts[4].trim());
                double volume = Double.parseDouble(parts[5].trim());

                // Compute the aggregation bucket (hour or day bucket)
                long bucketTimestamp = timestamp - (timestamp % bucketSize);
                buckets.computeIfAbsent(bucketTimestamp, _ -> new ArrayList<>())
                        .add(new MinuteTick(timestamp, open, high, low, close, volume));
            }
        }

        List<HourlyData> aggregatedList = new ArrayList<>();
        for (Map.Entry<Long, List<MinuteTick>> entry : buckets.entrySet()) {
            long bucketTimestamp = entry.getKey();
            List<MinuteTick> ticks = entry.getValue();

            // Sort ticks to ensure correct open and close values
            ticks.sort(Comparator.comparingLong(mt -> mt.timestamp));

            double open = ticks.get(0).open;
            double close = ticks.get(ticks.size() - 1).close;
            double high = ticks.stream().mapToDouble(t -> t.high).max().orElseThrow();
            double low = ticks.stream().mapToDouble(t -> t.low).min().orElseThrow();
            double volume = ticks.stream().mapToDouble(t -> t.volume).sum();

            aggregatedList.add(new HourlyData(bucketTimestamp, open, high, low, close, volume));
        }

        // Ensure the final aggregated list is sorted by timestamp.
        aggregatedList.sort(Comparator.comparingLong(data -> data.timestamp));

        return aggregatedList;
    }

    /**
     * Generates an hourly data file from the minute data file.
     *
     * @param minuteDataPath Path to the minute data file.
     * @param hourlyDataPath Path to the output hourly data file.
     * @throws IOException If there is an error reading or writing files.
     */
    public void generateHourlyFileFromMinuteFile(Path minuteDataPath, Path hourlyDataPath) throws IOException {
        generateFileFromMinuteFile(minuteDataPath, hourlyDataPath, AggregationType.HOURLY);
    }

    public enum AggregationType {
        HOURLY, DAILY
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
 * Represents a single row of aggregated data with OHLC (Open, High, Low, Close) and volume values.
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
     * Converts this aggregated data instance to a CSV-formatted row.
     *
     * @return CSV row string.
     */
    public String toCsvRow() {
        return String.format("%.1f,%.2f,%.2f,%.2f,%.2f,%.2f",
                (double) timestamp, open, high, low, close, volume);
    }
}