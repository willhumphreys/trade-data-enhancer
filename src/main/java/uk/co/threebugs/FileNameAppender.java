package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A utility class that appends the file name (without extension)
 * as the second column of a CSV file.
 */
@Slf4j
public class FileNameAppender {

    /**
     * Appends the file name as the second column for every row of the input file.
     *
     * @param inputPath  Path to the input file.
     * @param outputPath Path to the output file.
     * @param fileName   The name of the file being processed (without extension).
     */
    public void addFileNameColumn(Path inputPath, Path outputPath, String fileName) {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            log.info("Reading input file at: {}", inputPath);

            // Process the header row
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("The input file is empty.");
            }

            // Add 'name' column to the header
            writer.write("dateTime,name," + header.substring(header.indexOf(",") + 1)); // Insert after the 'dateTime' column
            writer.newLine();

            // Process data rows
            int processedCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String formattedLine = addFileNameToRow(line, fileName);
                writer.write(formattedLine);
                writer.newLine();
                processedCount++;
            }

            log.info("Processed {} rows and appended file name as a column.", processedCount);
        } catch (IOException e) {
            log.error("Error processing file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds the file name as the second column in a given row.
     *
     * @param row      A CSV row as a string.
     * @param fileName The name of the file (without extension) to add as the second column.
     * @return A new CSV row with the file name inserted as the second column.
     */
    private String addFileNameToRow(String row, String fileName) {
        String[] columns = row.split(",", 2); // Split into two parts to preserve the rest of the row after the first column
        if (columns.length < 2) {
            log.warn("Skipping invalid row: {}", row);
            return row; // Return the original row as-is for invalid rows
        }

        return columns[0] + "," + fileName + "," + columns[1]; // Insert the file name after the first column
    }
}