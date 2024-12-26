package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A utility class that appends a "weighting" column to a CSV file based on closing prices.
 */
@Slf4j
public class WeightingColumnAppender {

    private static final String DEFAULT_WEIGHTING = "1.00";

    /**
     * Appends a weighting column derived from the closing price to the input file.
     *
     * @param inputPath  Path to the input CSV file.
     * @param outputPath Path to the output file with the "weighting" column added.
     */
    public void addWeightingColumn(Path inputPath, Path outputPath) {
        var tickWeigher = new TickWeigher();

        try (var reader = Files.newBufferedReader(inputPath); var writer = Files.newBufferedWriter(outputPath)) {

            log.info("Reading input file at: {}", inputPath);

            // Process the header row
            var header = reader.readLine();
            if (header == null || header.isEmpty()) {
                throw new IOException("The input file is empty or has no header.");
            }

            // Dynamically locate the "Close" column
            var closePriceColumn = findColumnIndex(header, "Close");
            if (closePriceColumn == -1) {
                throw new IOException("The input file has no 'Close' column in the header.");
            }
            log.info("Close price column located at index: {}", closePriceColumn);

            // Add the "weighting" column to the header
            writer.write(header + ",weighting");
            writer.newLine();

            // Process each data row
            var processedCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                var formattedLine = addWeightingToRow(line, tickWeigher, closePriceColumn);
                writer.write(formattedLine);
                writer.newLine();
                processedCount++;
            }

            log.info("Processed {} rows and added weighting.", processedCount);
        } catch (IOException e) {
            log.error("Error processing file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Dynamically finds the index of the specified column name in the header row.
     *
     * @param headerRow  The header row of the CSV file as a string.
     * @param columnName The name of the column to locate.
     * @return The zero-based index of the column or -1 if not found.
     */
    private int findColumnIndex(String headerRow, String columnName) {
        var columns = headerRow.split(","); // Split the header into columns
        for (var i = 0; i < columns.length; i++) {
            if (columns[i].trim().equalsIgnoreCase(columnName)) {
                return i; // Return the index of the matching column
            }
        }
        return -1; // Column not found
    }

    /**
     * Adds the weighting column to a row using the TickWeigher.
     *
     * @param row              A CSV row as a string.
     * @param tickWeigher      The TickWeigher for calculating weightings.
     * @param closePriceColumn The index of the "Close" column.
     * @return A new CSV row with the weighting column appended.
     */
    private String addWeightingToRow(String row, TickWeigher tickWeigher, int closePriceColumn) {
        var columns = row.split(","); // Split the row into columns

        if (closePriceColumn >= columns.length) {
            log.warn("Row has insufficient columns; skipping: {}", row);
            return row + "," + DEFAULT_WEIGHTING; // Default weighting for rows that are invalid
        }

        try {
            var closePrice = new BigDecimal(columns[closePriceColumn].trim());
            var weighting = tickWeigher.getWeighting(closePrice);

            BigDecimal formattedWeighting = new BigDecimal(weighting); // Convert from String to BigDecimal to reformat
            return row + "," + formattedWeighting;

        } catch (NumberFormatException e) {
            log.warn("Skipping row with invalid close price: {}", row, e);
            return row + "," + DEFAULT_WEIGHTING; // Default weighting for rows with invalid close prices
        }
    }
}