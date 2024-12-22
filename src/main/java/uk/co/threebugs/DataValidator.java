package uk.co.threebugs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * DataValidator ensures the correctness of the input data file by:
 * - Verifying mandatory columns in the header.
 * - Writing cleaned data to a validated output.
 * - Writing invalid rows or columns to a separate file for review.
 */
public class DataValidator {

    private static final List<String> REQUIRED_COLUMNS = List.of("Timestamp", "Open", "High", "Low", "Close", "Volume");

    /**
     * Validates the input file, produces a cleaned version, and writes invalid rows/columns to a separate file.
     *
     * @param inputPath     Path to the input data file.
     * @param validatedPath Path to the clean data file.
     * @param invalidPath   Path to the file containing invalid data for review.
     * @throws IOException if file reading or writing fails.
     */
    public void validateDataFile(Path inputPath, Path validatedPath, Path invalidPath) throws IOException {
        try (var reader = Files.newBufferedReader(inputPath);
             var validatedWriter = Files.newBufferedWriter(validatedPath);
             var invalidWriter = Files.newBufferedWriter(invalidPath)) {

            // Read the header and validate columns
            var header = reader.readLine();
            if (header == null) {
                throw new IOException("The input file is empty.");
            }

            var columns = Arrays.asList(header.split(","));
            validateColumns(columns, invalidWriter);

            // Write the cleaned header to the validated file
            validatedWriter.write(String.join(",", REQUIRED_COLUMNS));
            validatedWriter.newLine();

            // Stream through the data, validating rows
            String line;
            while ((line = reader.readLine()) != null) {
                var fields = line.split(",");

                // If the row is valid, write it to the validated file; otherwise, write to the invalid file
                if (isValidRow(fields, columns)) {
                    validatedWriter.write(formatValidatedRow(fields, columns));
                    validatedWriter.newLine();
                } else {
                    invalidWriter.write(line);
                    invalidWriter.newLine();
                }
            }
        }
    }

    /**
     * Validates the column headers.
     *
     * @param columns       The list of columns read from the CSV file.
     * @param invalidWriter The writer to record invalid column information.
     * @throws IOException if writing to the invalid file fails.
     */
    private void validateColumns(List<String> columns, BufferedWriter invalidWriter) throws IOException {
        for (var col : columns) {
            if (!REQUIRED_COLUMNS.contains(col)) {
                invalidWriter.write("Invalid column: " + col);
                invalidWriter.newLine();
            }
        }

        // Check for missing mandatory columns
        for (var required : REQUIRED_COLUMNS) {
            if (!columns.contains(required)) {
                invalidWriter.write("Missing required column: " + required);
                invalidWriter.newLine();
            }
        }
    }

    /**
     * Determines if a row is valid by ensuring all mandatory columns exist and are formatted correctly.
     *
     * @param fields  The values for a given row.
     * @param columns The column headers.
     * @return true if the row is valid; false otherwise.
     */
    private boolean isValidRow(String[] fields, List<String> columns) {
        try {
            // Ensure mandatory fields exist
            for (var required : REQUIRED_COLUMNS) {
                var index = columns.indexOf(required);
                if (index < 0 || index >= fields.length || fields[index].isEmpty()) {
                    return false; // Missing required field
                }

                // Special validation for Timestamp (must be a number)
                if (required.equals("Timestamp")) {
                    Double.parseDouble(fields[index]); // Ensure it's a valid numeric value
                }
            }
        } catch (NumberFormatException e) {
            return false; // Invalid numeric field
        }

        return true;
    }

    /**
     * Formats a validated row with only the mandatory fields, in the correct order.
     *
     * @param fields  The original row's fields.
     * @param columns The column headers.
     * @return A string with the validated row.
     */
    private String formatValidatedRow(String[] fields, List<String> columns) {
        var builder = new StringBuilder();
        for (var required : REQUIRED_COLUMNS) {
            var index = columns.indexOf(required);
            builder.append(fields[index]).append(',');
        }
        // Remove the trailing comma
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }
}