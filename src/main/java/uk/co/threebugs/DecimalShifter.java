package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.NoSuchElementException;

@Slf4j
public class DecimalShifter {

    private static final String[] TARGET_COLUMNS = {"Open", "High", "Low", "Close"};
    private static final String TIMESTAMP_COLUMN = "Timestamp"; // Define timestamp column name

    /**
     * Shifts decimal places and returns the number of places shifted.
     * Also formats the timestamp column to avoid scientific notation.
     *
     * @param inputPath  Path to the input file.
     * @param outputPath Path to the output file with adjusted decimals.
     * @return Number of decimal places the data was shifted.
     */
    public int shiftDecimalPlaces(Path inputPath, Path outputPath) {
        try (var reader = Files.newBufferedReader(inputPath);
             var writer = Files.newBufferedWriter(outputPath)) {

            log.info("Reading input file at: {}", inputPath);

            // Process the header row and determine column indices
            var header = reader.readLine();
            if (header == null || header.isEmpty()) {
                throw new IOException("The input file is empty or has no header.");
            }

            writer.write(header);
            writer.newLine();

            var columnIndices = findTargetColumnIndices(header);
            int timestampIndex = getColumnIndex(header, TIMESTAMP_COLUMN); // Get timestamp index
            log.info("Located column indices for shift: {}", Arrays.toString(columnIndices));
            log.info("Located timestamp column index: {}", timestampIndex);

            // Calculate the maximum decimal places across all rows
            // Need to pass the original reader here as it's positioned after the header
            int maxDecimalPlaces = calculateMaxDecimalPlaces(reader, columnIndices);
            log.info("Maximum decimal places found: {}", maxDecimalPlaces);

            // Reset reader to process from the beginning again
            reader.close(); // Close the initial reader
            try (var refreshedReader = Files.newBufferedReader(inputPath)) {
                refreshedReader.readLine(); // Skip header again
                String line;
                while ((line = refreshedReader.readLine()) != null) {
                    // First, format the timestamp
                    String[] fields = line.split(",");
                    if (timestampIndex < fields.length) {
                        try {
                            double tsDouble = Double.parseDouble(fields[timestampIndex]);
                            long tsLong = (long) tsDouble;
                            fields[timestampIndex] = String.valueOf(tsLong);
                            line = String.join(",", fields); // Reconstruct line with formatted timestamp
                        } catch (NumberFormatException e) {
                            log.warn("Could not parse timestamp in line, skipping timestamp formatting: {}", line);
                            // Continue without timestamp formatting for this line
                        }
                    } else {
                        log.warn("Timestamp column index {} out of bounds for line, skipping timestamp formatting: {}", timestampIndex, line);
                    }

                    // Then, shift the decimals in the target columns
                    var updatedLine = RowShifter.shiftRow(line, columnIndices, maxDecimalPlaces);
                    writer.write(updatedLine);
                    writer.newLine();
                }
            }

            log.info("Decimal shift completed successfully. Output written to: {}", outputPath);
            return maxDecimalPlaces; // Return the number of decimal places shifted

        } catch (IOException e) {
            log.error("Error processing file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private int[] findTargetColumnIndices(String header) {
        var columns = header.split(",");
        return Arrays.stream(TARGET_COLUMNS)
                .mapToInt(columnName -> {
                    for (int i = 0; i < columns.length; i++) {
                        if (columns[i].trim().equalsIgnoreCase(columnName)) {
                            return i;
                        }
                    }
                    // Log or handle if a target column is not found, returning -1 for now
                    log.warn("Target column '{}' not found in header: {}", columnName, header);
                    return -1;
                })
                .filter(index -> index != -1) // Filter out indices where column wasn't found
                .toArray();
    }

    // Helper method to find a single column index
    private int getColumnIndex(String header, String columnName) {
        var columns = header.split(",");
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].trim().equalsIgnoreCase(columnName.trim())) {
                return i;
            }
        }
        throw new NoSuchElementException("Column '" + columnName + "' not found in header: " + header);
    }


    private int calculateMaxDecimalPlaces(BufferedReader reader, int[] columnIndices) throws IOException {
        int maxDecimalPlaces = 0;
        String line;
        long rowNum = 1; // Start after header
        while ((line = reader.readLine()) != null) {
            rowNum++;
            var columns = line.split(",");
            for (int columnIndex : columnIndices) {
                // Check if columnIndex is valid and within bounds
                if (columnIndex < 0 || columnIndex >= columns.length) {
                    log.warn("Column index {} is invalid or out of bounds for row {}. Line: {}", columnIndex, rowNum, line);
                    continue; // Skip this column index for this row
                }
                try {
                    // Trim whitespace before parsing
                    var valueStr = columns[columnIndex].trim();
                    if (valueStr.isEmpty()) {
                        log.warn("Empty value found in target column {} at row {}. Line: {}", columnIndex, rowNum, line);
                        continue; // Skip empty values
                    }
                    var value = new BigDecimal(valueStr);
                    int scale = value.scale();
                    if (scale < 0) { // Handle scientific notation scale if necessary
                        scale = Math.max(0, value.precision() - value.unscaledValue().toString().length());
                    }
                    if (scale > maxDecimalPlaces) {
                        maxDecimalPlaces = scale;
                        // Avoid logging excessively if maxDecimalPlaces updates frequently
                        // log.debug("New maximum decimal places found: {} (value: {}) at row {}", maxDecimalPlaces, value, rowNum);
                    }
                } catch (NumberFormatException e) {
                    // Log the problematic value and row number
                    log.error("Invalid number format in column {} at row {}. Value: '{}'. Line: {}", columnIndex, rowNum, columns[columnIndex], line, e);
                    throw new RuntimeException("Invalid or non-numeric value in row: " + rowNum + ", column index: " + columnIndex + ", value: '" + columns[columnIndex] + "'", e);
                } catch (ArrayIndexOutOfBoundsException e) {
                    // This shouldn't happen with the bounds check, but good practice
                    log.error("Array index out of bounds accessing column {} at row {}. Line: {}", columnIndex, rowNum, line, e);
                    throw new RuntimeException("Error accessing column data at row: " + rowNum + ", column index: " + columnIndex, e);
                }
            }
        }
        log.info("Finished calculating max decimal places: {}", maxDecimalPlaces);
        return maxDecimalPlaces;
    }


    /**
     * Adjusts decimal places in the input file using a predefined shift value.
     * Also formats the timestamp column to avoid scientific notation.
     *
     * @param inputPath    Path to the input file.
     * @param outputPath   Path to the output file with adjusted decimals.
     * @param decimalShift Predefined shift value (number of decimal places to shift).
     */
    public void shiftDecimalPlacesWithPredefinedShift(Path inputPath, Path outputPath, int decimalShift) {
        try (var reader = Files.newBufferedReader(inputPath);
             var writer = Files.newBufferedWriter(outputPath)) {

            log.info("Shifting decimal places using predefined value: {}.", decimalShift);

            // Read the header row
            var header = reader.readLine();
            if (header == null || header.isEmpty()) {
                throw new IOException("The input file is empty or has no header.");
            }

            writer.write(header);
            writer.newLine();

            // Determine target columns for shifting and timestamp column
            var columnIndices = findTargetColumnIndices(header);
            int timestampIndex = getColumnIndex(header, TIMESTAMP_COLUMN); // Get timestamp index
            log.info("Located column indices for predefined shift: {}", Arrays.toString(columnIndices));
            log.info("Located timestamp column index: {}", timestampIndex);


            // Apply the predefined decimal shift and format timestamp for all rows
            String line;
            while ((line = reader.readLine()) != null) {
                // First, format the timestamp
                String[] fields = line.split(",");
                if (timestampIndex < fields.length) {
                    try {
                        double tsDouble = Double.parseDouble(fields[timestampIndex]);
                        long tsLong = (long) tsDouble;
                        fields[timestampIndex] = String.valueOf(tsLong);
                        line = String.join(",", fields); // Reconstruct line with formatted timestamp
                    } catch (NumberFormatException e) {
                        log.warn("Could not parse timestamp in line, skipping timestamp formatting: {}", line);
                        // Continue without timestamp formatting for this line
                    }
                } else {
                    log.warn("Timestamp column index {} out of bounds for line, skipping timestamp formatting: {}", timestampIndex, line);
                }

                // Then, shift the decimals in the target columns using the modified line
                var updatedLine = RowShifter.shiftRow(line, columnIndices, decimalShift);
                writer.write(updatedLine);
                writer.newLine();
            }

            log.info("Decimal adjustment completed successfully. Output written to: {}", outputPath);

        } catch (IOException e) {
            log.error("Error processing file: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}