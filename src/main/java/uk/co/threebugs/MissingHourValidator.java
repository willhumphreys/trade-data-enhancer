package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class MissingHourValidator {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    /**
     * Validates that there are no missing hourly records in the file while allowing in-between minute ticks.
     * Does not expect hourly ticks prior to the first tick.
     *
     * @param filePath Path to the file to be validated.
     * @throws IOException           If an error occurs during file processing.
     * @throws IllegalStateException If any hourly ticks are missing.
     */
    public void validateHourlyRecords(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath);

        if (lines.isEmpty()) {
            throw new IllegalStateException("File is empty, cannot validate hourly records.");
        }

        // Skip the header row
        boolean hasHourTick = true; // Track if HH:00 tick exists for the current hour
        LocalDateTime currentHour = null; // The current hour being processed

        for (int i = 1; i < lines.size(); i++) { // Start after the header
            String[] row = lines.get(i).split(",");
            String timestamp = row[0].trim();
            LocalDateTime tickDateTime = LocalDateTime.parse(timestamp, DATE_TIME_FORMATTER);

            // Align tickDateTime to the start of the hour (HH:00)
            LocalDateTime tickHour = tickDateTime.withMinute(0).withSecond(0);

            // First tick initialization
            if (currentHour == null) {
                currentHour = tickHour; // Define the start of validation based on the first tick
            } else if (!tickHour.equals(currentHour)) {
                // A new hour has started, validate the previous hour
                if (!hasHourTick) {
                    throw new IllegalStateException(String.format(
                            "Missing tick for the hour starting at %s.",
                            currentHour.format(DATE_TIME_FORMATTER)
                    ));
                }

                hasHourTick = false;
                currentHour = tickHour; // Update to the new hour
            }

            if (tickDateTime.equals(tickHour)) {
                hasHourTick = true; // Mark that HH:00 tick exists for this hour
            }
        }

        // Validate the final hour (if the file ends mid-hour, check if HH:00 existed)
        if (!hasHourTick) {
            throw new IllegalStateException(String.format(
                    "Missing tick for the hour starting at %s.",
                    currentHour.format(DATE_TIME_FORMATTER)
            ));
        }

        // Validation passed
    }
}