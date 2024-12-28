package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class MapTimeColumnAdder {

    // DateTimeFormatter for parsing and formatting "yyyy-MM-ddTHH:mm"
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    public void addMapTimeColumnAndCheckHourlyTrades(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            log.info("Processing file to verify trades with minute == 0 and append 'mapTime' column: {}", inputPath);

            // Read the header and write it to the output file with the new column appended
            String header = reader.readLine();
            if (header == null || header.trim().isEmpty()) {
                throw new IOException("Input file is empty or missing a header row");
            }
            writer.write(header + ",mapTime");
            writer.newLine();

            // Variables for processing trades
            String currentLine;
            LocalDateTime lastHourTradeTime = null; // Keeps track of the last trade with minute == 0

            while ((currentLine = reader.readLine()) != null) {
                // Extract the LocalDateTime from the row
                LocalDateTime currentTime = extractDateTime(currentLine);

                // Check trades with minute == 0
                if (currentTime != null && currentTime.getMinute() == 0) {
                    if (lastHourTradeTime != null) {
                        // Verify that there's a trade exactly one hour after the previous trade
                        if (!currentTime.equals(lastHourTradeTime.plusHours(1))) {
                            log.warn("Trade gap detected: Expected trade at {}, but found {}", lastHourTradeTime.plusHours(1), currentTime);
                        }
                    }
                    // Update lastHourTradeTime to the current trade time
                    lastHourTradeTime = currentTime;
                }

                // Append mapTime and write the row to the output file
                String updatedLine = appendMapTimeColumn(currentLine, currentTime);
                writer.write(updatedLine);
                writer.newLine();
            }

            log.info("Processing completed successfully. Output written to: {}", outputPath);
        } catch (IOException e) {
            log.error("Error processing file: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Extracts the dateTime from a given row, assuming the first column holds the dateTime in "yyyy-MM-ddTHH:mm" format.
     *
     * @param row The row of text.
     * @return LocalDateTime parsed from the row, or null if parsing fails.
     */
    private LocalDateTime extractDateTime(String row) {
        try {
            String[] columns = row.split(",");
            if (columns.length < 1) {
                log.warn("Skipping invalid row due to insufficient columns: {}", row);
                return null;
            }

            // First column is expected to be the dateTime
            String dateTimeString = columns[0].trim();
            return LocalDateTime.parse(dateTimeString, DATE_TIME_FORMATTER);
        } catch (Exception e) {
            log.warn("Failed to parse dateTime in row: {}. Error: {}", row, e.getMessage());
            return null;
        }
    }

    /**
     * Appends the mapTime column to the row using the provided LocalDateTime.
     *
     * @param row      The original row of text.
     * @param dateTime The LocalDateTime to append as mapTime.
     * @return The updated row with the mapTime column appended.
     */
    private String appendMapTimeColumn(String row, LocalDateTime dateTime) {
        if (dateTime == null) {
            return row + ",null"; // Append null if dateTime is invalid
        }
        return row + "," + dateTime.format(DATE_TIME_FORMATTER); // Append the formatted dateTime
    }
}