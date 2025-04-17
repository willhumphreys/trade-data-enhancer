package uk.co.threebugs.preconvert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PolygonDataConverter implements SourceDataConverter {
    @Override
    public void convert(Path input, Path output) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(input);
             BufferedWriter writer = Files.newBufferedWriter(output)) {

            // Write header for output file
            writer.write("Timestamp,Open,High,Low,Close,Volume");
            writer.newLine();

            // Read header line and find column indices
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return; // Empty file
            }

            String[] headers = headerLine.split(",");

            // Find indices of required columns
            int volumeIndex = findColumnIndex(headers, "v");
            int vwapIndex = findColumnIndex(headers, "vw");  // vw is volume weighted average price
            int openIndex = findColumnIndex(headers, "o");
            int closeIndex = findColumnIndex(headers, "c");
            int highIndex = findColumnIndex(headers, "h");
            int lowIndex = findColumnIndex(headers, "l");
            int timestampIndex = findColumnIndex(headers, "t");
            int numTradesIndex = findColumnIndex(headers, "n");  // n is number of trades

            // Check that all required columns were found
            if (openIndex == -1 || closeIndex == -1 || highIndex == -1 ||
                    lowIndex == -1 || volumeIndex == -1 || timestampIndex == -1) {
                throw new IOException("Required columns missing from input file");
            }

            // Process each line
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue; // Skip empty lines
                }

                String[] parts = line.split(",");
                if (parts.length < Math.max(Math.max(Math.max(openIndex, closeIndex),
                                Math.max(highIndex, lowIndex)),
                        Math.max(volumeIndex, timestampIndex)) + 1) {
                    continue; // Skip malformed lines
                }

                try {
                    // Extract values from input format using discovered indices
                    double open = Double.parseDouble(parts[openIndex]);
                    double high = Double.parseDouble(parts[highIndex]);
                    double low = Double.parseDouble(parts[lowIndex]);
                    double close = Double.parseDouble(parts[closeIndex]);
                    double volume = Double.parseDouble(parts[volumeIndex]);

                    // Convert timestamp from milliseconds to seconds
                    long timestampMs = Long.parseLong(parts[timestampIndex]);
                    double timestampSec = timestampMs / 1000.0;

                    // Directly concatenate values as strings to preserve decimal places
                    String outputLine = timestampSec + "," +
                            open + "," +
                            high + "," +
                            low + "," +
                            close + "," +
                            volume;

                    writer.write(outputLine);
                    writer.newLine();
                } catch (NumberFormatException e) {
                    // Log error and continue with next line
                    System.err.println("Error parsing line: " + line);
                }
            }
        }
    }

    /**
     * Helper method to find a column's index in the header array
     *
     * @param headers    Array of header strings
     * @param columnName Name of column to find
     * @return Index of the column or -1 if not found
     */
    private int findColumnIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }
}