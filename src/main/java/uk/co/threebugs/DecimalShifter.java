package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

@Slf4j
public class DecimalShifter {

    private static final String[] TARGET_COLUMNS = {"Open", "High", "Low", "Close"};

    /**
     * Shifts decimal places and returns the number of places shifted.
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
            log.info("Located column indices for shift: {}", Arrays.toString(columnIndices));

            // Calculate the maximum decimal places across all rows
            int maxDecimalPlaces = calculateMaxDecimalPlaces(reader, columnIndices);
            log.info("Maximum decimal places found: {}", maxDecimalPlaces);

            // Shift decimal places and write updated rows into the output file
            reader.close(); // Reset reader
            try (var refreshedReader = Files.newBufferedReader(inputPath)) {
                refreshedReader.readLine(); // Skip header
                String line;
                while ((line = refreshedReader.readLine()) != null) {
                    var updatedLine = shiftRow(line, columnIndices, maxDecimalPlaces);
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
                    return -1;
                })
                .toArray();
    }

    private int calculateMaxDecimalPlaces(BufferedReader reader, int[] columnIndices) throws IOException {
        int maxDecimalPlaces = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            var columns = line.split(",");
            for (int columnIndex : columnIndices) {
                if (columnIndex == -1 || columnIndex >= columns.length) {
                    continue;
                }
                try {
                    var value = new BigDecimal(columns[columnIndex].trim());
                    maxDecimalPlaces = Math.max(maxDecimalPlaces, value.scale());
                } catch (NumberFormatException row) {
                    throw new RuntimeException("Invalid or non-numeric value in row: " + row);
                }
            }
        }
        return maxDecimalPlaces;
    }

    private String shiftRow(String row, int[] columnIndices, int maxDecimalPlaces) {
        var multiplier = BigDecimal.TEN.pow(maxDecimalPlaces);
        var columns = row.split(",");
        for (int columnIndex : columnIndices) {
            if (columnIndex == -1 || columnIndex >= columns.length) {
                continue;
            }
            try {
                var value = new BigDecimal(columns[columnIndex].trim());
                // Multiply and round, then format as non-scientific string
                var shiftedValue = value.multiply(multiplier).setScale(0, RoundingMode.HALF_UP).toPlainString();
                columns[columnIndex] = shiftedValue; // Use full numeric representation
            } catch (NumberFormatException e) {
                log.warn("Invalid or non-numeric value in row: {}", row);
                throw new RuntimeException("Invalid or non-numeric value in row: " + row);
            }
        }
        return String.join(",", columns);
    }

    /**
     * Adjusts decimal places in the input file using a predefined shift value.
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

            // Determine target columns for shifting
            var columnIndices = findTargetColumnIndices(header);
            log.info("Located column indices for predefined shift: {}", Arrays.toString(columnIndices));

            // Apply the predefined decimal shift to all rows
            String line;
            while ((line = reader.readLine()) != null) {
                var updatedLine = shiftRow(line, columnIndices, decimalShift);
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