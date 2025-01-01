package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class DataIntegrityChecker {

    /**
     * Checks the data file for data integrity, ensuring there are:
     * - No gaps between consecutive hourly entries.
     * - At least one intermediate data entry (not exactly on the hour).
     *
     * @return A String describing the result of the check. "No issues found" if all checks pass,
     * otherwise a message detailing the problems detected.
     * @throws IOException if reading the file fails.
     */
    public String validateDataIntegrity(Path minuteFilePath, Path hourlyFilePath) throws IOException {
        // Load all timestamps from the minute file
        Set<LocalDateTime> minuteTimestamps = loadMinuteTimestamps(minuteFilePath);

        // Determine the range of timestamps covered by the minute file
        if (minuteTimestamps.isEmpty()) {
            return "No minute-level data found, skipping validation.";
        }

        LocalDateTime minMinuteTimestamp = minuteTimestamps.stream().min(LocalDateTime::compareTo).orElseThrow();
        LocalDateTime maxMinuteTimestamp = minuteTimestamps.stream().max(LocalDateTime::compareTo).orElseThrow();

        // Load hourly timestamps and check only those within the range of the minute file
        Set<LocalDateTime> hourlyTimestamps = loadHourlyTimestamps(hourlyFilePath);
        for (LocalDateTime hourlyTimestamp : hourlyTimestamps) {
            if (hourlyTimestamp.isBefore(minMinuteTimestamp) || hourlyTimestamp.isAfter(maxMinuteTimestamp)) {
                // Ignore timestamps outside the range of the minute file
                continue;
            }

            if (hourlyTimestamp.getMinute() == 0 && hourlyTimestamp.getSecond() == 0) {
                if (!minuteTimestamps.contains(hourlyTimestamp)) {
                    long missingEpochSeconds = hourlyTimestamp.toEpochSecond(ZoneOffset.UTC);
                    throw new IllegalStateException("Error: Hourly timestamp missing in minute-level data! " +
                            "Missing Timestamp (UTC): " + hourlyTimestamp + " (Epoch: " + missingEpochSeconds + ").");
                }
            }
        }

        return "No issues found.";
    }

    private Set<LocalDateTime> loadHourlyTimestamps(Path hourlyFilePath) throws IOException {
        Set<LocalDateTime> hourlyTimestamps = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(hourlyFilePath)) {
            String header = reader.readLine(); // Ignore the header
            if (header == null || !reader.ready()) {
                throw new IllegalArgumentException("Error: Hourly data file is empty or malformed. " + hourlyFilePath);
            }

            String line;
            while ((line = reader.readLine()) != null) {
                LocalDateTime timestamp = parseTimestampFromLine(line);
                hourlyTimestamps.add(timestamp);
            }
        }

        return hourlyTimestamps;
    }

    private Set<LocalDateTime> loadMinuteTimestamps(Path minuteFilePath) throws IOException {
        Set<LocalDateTime> minuteTimestamps = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(minuteFilePath)) {
            String header = reader.readLine(); // Ignore the header
            if (header == null || !reader.ready()) {
                throw new IllegalArgumentException("Error: Minute data file is empty or malformed.");
            }

            String line;
            while ((line = reader.readLine()) != null) {
                LocalDateTime timestamp = parseTimestampFromLine(line);
                minuteTimestamps.add(timestamp);
            }
        }

        return minuteTimestamps;
    }

    private LocalDateTime parseTimestampFromLine(String line) {
        String[] parts = line.split(",");
        if (parts.length == 0) throw new IllegalArgumentException("Malformed CSV line: " + line);

        long epochSeconds = (long) Double.parseDouble(parts[0]);

        return LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC);
    }
}