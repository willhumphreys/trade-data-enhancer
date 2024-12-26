package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class NewHourColumnAdder {

    public void addNewHourColumn(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            log.info("Adding 'newHour' column to data from file: {}", inputPath);

            String header = reader.readLine();
            if (header == null || header.trim().isEmpty()) {
                throw new IOException("Input file is empty or missing a header row");
            }

            // Write updated header with the new column
            writer.write(header + ",newHour");
            writer.newLine();

            String line;
            while ((line = reader.readLine()) != null) {
                String updatedLine = processRow(line);
                writer.write(updatedLine);
                writer.newLine();
            }

            log.info("Successfully added 'newHour' column. Output written to: {}", outputPath);
        } catch (IOException e) {
            log.error("Error processing file for 'newHour' column: {}", e.getMessage());
            throw e;
        }
    }

    private String processRow(String row) {
        String[] columns = row.split(",");

        if (columns.length < 1) {
            log.warn("Skipping invalid row due to insufficient columns: {}", row);
            return row + ",false"; // Append false for invalid row
        }

        try {
            // Assuming "dateTime" is the first column
            String dateTime = columns[0].trim();
            boolean newHour = isNewHour(dateTime);
            return row + "," + newHour; // Append the `newHour` column to the row
        } catch (Exception e) {
            log.warn("Error processing row for 'newHour': {}", row);
            return row + ",false"; // Safely append false if an exception occurs
        }
    }

    private boolean isNewHour(String dateTime) {
        // Assuming dateTime is in the format "yyyy-MM-ddTHH:mm"
        String[] dateTimeParts = dateTime.split("T");
        if (dateTimeParts.length != 2) {
            log.warn("Invalid dateTime format: {}", dateTime);
            return false; // Return false for invalid dateTime format
        }

        String[] timeParts = dateTimeParts[1].split(":");
        if (timeParts.length != 2) {
            log.warn("Invalid time format in dateTime: {}", dateTime);
            return false; // Return false for invalid time format
        }

        // Check if the minute part is "00"
        return "00".equals(timeParts[1]);
    }
}