package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

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

            // Dynamically find column indices from the header row
            Map<String, Integer> columnIndexMap = getColumnIndexMap(header);

            Integer highColIndex = columnIndexMap.get("high");
            Integer lowColIndex = columnIndexMap.get("low");

            // Validate that required columns are present
            if (highColIndex == null || lowColIndex == null) {
                throw new IllegalArgumentException("Input file is missing required 'High' or 'Low' columns");
            }

            // Write updated header with the new columns
            writer.write(header + ",fixedLow,fixedHigh");
            writer.newLine();

            String previousLine = null;
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String updatedLine = processRow(previousLine, currentLine, highColIndex, lowColIndex);
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

    private Map<String, Integer> getColumnIndexMap(String header) {
        String[] columns = header.split(",");
        Map<String, Integer> columnIndexMap = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            columnIndexMap.put(columns[i].trim(), i); // Trim to avoid issues with extra spaces
        }
        return columnIndexMap;
    }

    String processRow(String previousRow, String currentRow, int highColIndex, int lowColIndex) {
        String[] columns = currentRow.split(",");
        if (previousRow == null) {
            // For the first row, fixedLow == low, fixedHigh == high
            return currentRow + "," + columns[lowColIndex] + "," + columns[highColIndex];
        }

        String[] previousColumns = previousRow.split(",");
        long previousHigh = Long.parseLong(previousColumns[highColIndex]); // high of the previous row
        long previousLow = Long.parseLong(previousColumns[lowColIndex]); // low of the previous row
        long currentHigh = Long.parseLong(columns[highColIndex]); // high of the current row
        long currentLow = Long.parseLong(columns[lowColIndex]); // low of the current row

        // Adjust fixedLow and fixedHigh for long values
        long fixedLow = Math.max(previousLow, currentLow); // Ensure no gaps below
        long fixedHigh = Math.min(previousHigh, currentHigh); // Ensure no gaps above

        return currentRow + "," + fixedLow + "," + fixedHigh;
    }
}