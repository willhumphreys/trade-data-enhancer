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
     * Validates that there are no missing hourly records in the file.
     *
     * @param filePath Path to the file to be validated.
     * @throws IOException           If an error occurs during file processing.
     * @throws IllegalStateException If any hourly records are missing.
     */
    public void validateHourlyRecords(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath);

        if (lines.isEmpty()) {
            throw new IllegalStateException("File is empty, cannot validate hourly records.");
        }

        // Skip the header
        String previousTimestamp = null;

        for (int i = 1; i < lines.size(); i++) { // Skip the header (start from 1)
            String[] row = lines.get(i).split(",");

            String currentTimestamp = row[0].trim();
            LocalDateTime currentDateTime = LocalDateTime.parse(currentTimestamp, DATE_TIME_FORMATTER);

            if (previousTimestamp != null) {
                LocalDateTime previousDateTime = LocalDateTime.parse(previousTimestamp, DATE_TIME_FORMATTER);

                // Check for missing hours
                if (!previousDateTime.plusHours(1).equals(currentDateTime)) {
                    throw new IllegalStateException(String.format(
                            "Missing hourly record between %s and %s.",
                            previousDateTime.format(DATE_TIME_FORMATTER),
                            currentDateTime.format(DATE_TIME_FORMATTER)
                    ));
                }
            }

            previousTimestamp = currentTimestamp;
        }
    }
}