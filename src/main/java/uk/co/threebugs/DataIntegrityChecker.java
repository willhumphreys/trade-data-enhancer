package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class DataIntegrityChecker {

    /**
     * Checks the data file for data integrity, ensuring there are:
     * - No gaps between consecutive hourly entries.
     * - At least one intermediate data entry (not exactly on the hour).
     *
     * @param filePath Path to the file to be checked.
     * @return A String describing the result of the check. "No issues found" if all checks pass,
     * otherwise a message detailing the problems detected.
     * @throws IOException if reading the file fails.
     */
    public String validateDataIntegrity(Path filePath) throws IOException {
        boolean hasIntermediateData = false; // Tracks if we have any data that isn't on the hour
        LocalDateTime previousHourlyTimestamp = null;

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
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

                if (currentTimestamp.getMinute() != 0 || currentTimestamp.getSecond() != 0) {
                    // Found an intermediate entry (not exactly at HH:00)
                    hasIntermediateData = true;
                } else {
                    // Process only hourly entries
                    if (previousHourlyTimestamp != null && !previousHourlyTimestamp.plusHours(1).equals(currentTimestamp)) {
                        return "Error: Gap detected! Expected: " + previousHourlyTimestamp.plusHours(1) + ", Found: " + currentTimestamp + ".";
                    }
                    previousHourlyTimestamp = currentTimestamp; // Update hourly timestamp
                }
            }

            if (previousHourlyTimestamp == null) {
                return "Error: No valid hourly entries were found in the file.";
            }
        }

        if (!hasIntermediateData) {
            return "Error: No intermediate (non-hourly) data entries found in the file.";
        }

        return "No issues found.";
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