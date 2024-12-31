package uk.co.threebugs;

import java.io.*;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MissingHourAdder {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    /**
     * Adds missing holiday hourly rows to the data without removing existing ticks.
     *
     * @param inputPath  Path to the input file (CSV with ticks).
     * @param outputPath Path to write the output file with added rows.
     * @throws IOException If reading or writing fails.
     */
    public void addMissingHours(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath.toFile())); BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath.toFile()))) {
            String header = reader.readLine(); // Read the header
            if (header == null) {
                throw new IOException("Input file is empty.");
            }

            // Write the updated header with 'holiday' column to output
            writer.write(header + ",holiday");
            writer.newLine();

            String previousRow = null;  // Track the previous row to calculate gaps
            LocalDateTime previousTimestamp = null; // Track the timestamp of the previous row

            // Process each subsequent row in the input file
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                currentLine = currentLine.trim();

                // Split line and parse timestamp
                String[] fields = currentLine.split(",");
                int dateTimeIndex = getIndex(header, "dateTime");
                LocalDateTime currentTimestamp = LocalDateTime.parse(fields[dateTimeIndex], FORMATTER);

                // Align the current timestamp to the nearest hour (if needed, for consistency)
                currentTimestamp = currentTimestamp.withMinute(0).withSecond(0);

                if (previousTimestamp == null) {
                    // First row: Initialize previous timestamp
                    previousTimestamp = currentTimestamp; // Align to start of the hour
                    writer.write(currentLine + ",0"); // Write the first row with 'holiday = 0'
                    writer.newLine();
                    previousRow = currentLine;
                    continue;
                }

                // Fill in missing hourly rows between previousTimestamp and currentTimestamp
                LocalDateTime nextHour = previousTimestamp.plusHours(1); // Start with the next hour
                while (!nextHour.isAfter(currentTimestamp)) {
                    // Insert holiday tick if there's a gap
                    if (nextHour.equals(currentTimestamp)) {
                        break; // Don't add a holiday tick if it's the exact hour of the current row
                    }
                    // Generate an aligned holiday row
                    String holidayRow = createHolidayRow(header, nextHour, previousRow);
                    writer.write(holidayRow + ",1"); // Write holiday tick row with 'holiday = 1'
                    writer.newLine();
                    nextHour = nextHour.plusHours(1); // Move to the next hour
                }

                // Write the current row as an existing tick
                writer.write(currentLine + ",0"); // Write current row with 'holiday = 0'
                writer.newLine();

                // Update the previous row and timestamp for the next iteration
                previousRow = currentLine;
                previousTimestamp = currentTimestamp;
            }
        }
    }

    /**
     * Creates a holiday hourly row with most fields set to -1 for the missing hour.
     *
     * @param header       The headers of the CSV file.
     * @param dateTime     The missing hour timestamp.
     * @param referenceRow A reference row to match the structure for other fields.
     * @return A holiday row as a String.
     */
    private String createHolidayRow(String header, LocalDateTime dateTime, String referenceRow) {
        String[] headers = header.split(",");
        String[] referenceFields = referenceRow.split(",");
        StringBuilder newRow = new StringBuilder();

        for (int i = 0; i < headers.length; i++) {
            if ("dateTime".equals(headers[i]) || "mapTime".equals(headers[i])) {
                // For dateTime and mapTime fields, use the provided dateTime argument
                newRow.append(dateTime.withMinute(0).withSecond(0).format(FORMATTER));
            } else if ("name".equals(headers[i])) {
                // Use the 'name' field from the reference row
                newRow.append(referenceFields[getIndex(header, "name")]);
            } else if ("newHour".equals(headers[i])) {
                newRow.append("true");
            } else {
                // Set all other fields to -1
                newRow.append("-1");
            }

            if (i < headers.length - 1) {
                newRow.append(","); // Add commas between fields
            }
        }

        return newRow.toString();
    }

    /**
     * Helper method to find the index of a column in the header.
     *
     * @param header     The header row as a String.
     * @param columnName The name of the column.
     * @return The index of the column.
     */
    private int getIndex(String header, String columnName) {
        String[] headers = header.split(",");
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals(columnName)) {
                return i;
            }
        }
        throw new IllegalStateException("Column not found: " + columnName);
    }
}