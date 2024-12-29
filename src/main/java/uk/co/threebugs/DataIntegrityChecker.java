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
        boolean hasIntermediateData = false; // Tracks if we have any data that isn't on the hour
        LocalDateTime previousTimestamp = null;

        // Load hourly timestamps from the hourly file
        Set<LocalDateTime> hourlyTimestamps = loadHourlyTimestamps(hourlyFilePath);

        try (BufferedReader reader = Files.newBufferedReader(minuteFilePath)) {
            String header = reader.readLine(); // Read and ignore the header
            if (header == null) return "Error: The input data file is empty.";

            String line;
            while ((line = reader.readLine()) != null) {
                LocalDateTime currentTimestamp;
                try {
                    currentTimestamp = parseTimestampFromLine(line);
                } catch (Exception e) {
                    return "Error: Malformed CSV line: \"" + line + "\".";
                }

                // Check if it's an intermediate entry (not on the hour)
                if (currentTimestamp.getMinute() != 0 || currentTimestamp.getSecond() != 0) {
                    hasIntermediateData = true;
                } else {
                    // This is an hourly entry
                    if (previousTimestamp != null && !previousTimestamp.plusHours(1).equals(currentTimestamp)) {
                        // Ensure no gap exists on an hour where there is hourly data
                        LocalDateTime expectedTimestamp = previousTimestamp.plusHours(1);
                        if (hourlyTimestamps.contains(expectedTimestamp)) {
                            throw new IllegalStateException("Error: Gap detected for timestamp where hourly data exists! Expected: "
                                    + expectedTimestamp + ", Found: " + currentTimestamp + ".");
                        }
                    }
                    previousTimestamp = currentTimestamp;
                }
            }

            if (previousTimestamp == null) {
                return "Error: No valid hourly entries were found in the file.";
            }
        }

        if (!hasIntermediateData) {
            return "Error: No intermediate (non-hourly) data entries found in the file.";
        }

        return "No issues found.";
    }

    /**
     * Loads all hourly timestamps from the hourly data file into a Set.
     *
     * @param hourlyFilePath Path to the hourly data file.
     * @return A Set containing LocalDateTime objects for each parsed hourly timestamp.
     * @throws IOException if reading the file fails.
     */
    private Set<LocalDateTime> loadHourlyTimestamps(Path hourlyFilePath) throws IOException {
        Set<LocalDateTime> hourlyTimestamps = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(hourlyFilePath)) {
            String header = reader.readLine(); // Read and ignore the header
            if (header == null || !reader.ready()) {
                throw new IllegalArgumentException("Error: Hourly data file is empty or malformed.");
            }

            String line;
            while ((line = reader.readLine()) != null) {
                LocalDateTime timestamp = parseTimestampFromLine(line);
                hourlyTimestamps.add(timestamp); // Add the parsed timestamp
            }
        }

        return hourlyTimestamps;
    }

    /**
     * Parses a timestamp from a CSV line.
     *
     * @param line A single line from the CSV file.
     * @return The timestamp as a LocalDateTime.
     */
    private LocalDateTime parseTimestampFromLine(String line) {
        String[] parts = line.split(",");
        if (parts.length == 0) throw new IllegalArgumentException("Malformed CSV line: " + line);

        long epochSeconds = (long) Double.parseDouble(parts[0]);

        return LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC);
    }
}