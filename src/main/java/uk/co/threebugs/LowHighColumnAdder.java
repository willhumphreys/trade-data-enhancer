package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class LowHighColumnAdder {

    public void addFixedLowAndHighColumns(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            log.info("Adding 'fixedLow' and 'fixedHigh' columns to data from file: {}", inputPath);

            // Read header
            String header = reader.readLine();
            if (header == null || header.trim().isEmpty()) {
                throw new IOException("Input file is empty or missing a header row");
            }

            // Write updated header with the new columns
            writer.write(header + ",fixedLow,fixedHigh");
            writer.newLine();

            String previousLine = null;
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {

                String updatedLine = processRow(previousLine, currentLine);
                writer.write(updatedLine);
                writer.newLine();
                previousLine = currentLine; // Shift to the next row
            }

            log.info("Successfully added 'fixedLow' and 'fixedHigh' columns. Output written to: {}", outputPath);
        } catch (IOException e) {
            log.error("Error processing file for 'fixedLow' and 'fixedHigh' columns: {}", e.getMessage());
            throw e;
        }
    }

    private String processRow(String previousRow, String currentRow) {
        String[] columns = currentRow.split(",");
        if (previousRow == null) {
            // For the first row, fixedLow == low, fixedHigh == high
            return currentRow + "," + columns[4] + "," + columns[3];
        }

        String[] previousColumns = previousRow.split(",");
        double previousHigh = Double.parseDouble(previousColumns[3]); // high of the previous row
        double previousLow = Double.parseDouble(previousColumns[4]); // low of the previous row
        double currentHigh = Double.parseDouble(columns[3]); // high of the current row
        double currentLow = Double.parseDouble(columns[4]); // low of the current row

        // Adjust low and high if there are gaps
        double fixedLow = Math.max(currentLow, previousHigh); // Ensure no gaps above
        double fixedHigh = Math.min(currentHigh, previousLow); // Ensure no gaps below

        return currentRow + "," + fixedLow + "," + fixedHigh;
    }
}