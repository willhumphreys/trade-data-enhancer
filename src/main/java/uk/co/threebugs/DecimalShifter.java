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

    public void shiftDecimalPlaces(Path inputPath, Path outputPath) {
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

            // Shift decimal places and write updated rows to output
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
}