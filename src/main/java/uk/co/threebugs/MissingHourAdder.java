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
     * @return
     * @throws IOException If reading or writing fails.
     */
    public long addMissingHours(Path inputPath, Path outputPath) throws IOException {

        long addedCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath.toFile()));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath.toFile()))) {

            String header = reader.readLine(); // Read the header
            if (header == null) {
                throw new IOException("Input file is empty.");
            }

            // Write the updated header with the appended 'holiday' column
            writer.write(header + ",holiday");
            writer.newLine();

            String currentLine = reader.readLine(); // Read the first data row
            if (currentLine == null) {
                throw new IOException("Input file contains only a header.");
            }

            // Parse the first timestamp to determine the starting hour
            String[] firstRowFields = currentLine.split(",");
            int dateTimeIndex = getIndex(header, "dateTime");
            LocalDateTime firstTimestamp = LocalDateTime.parse(firstRowFields[dateTimeIndex], FORMATTER);

            // Align the first timestamp to the start of the hour
            LocalDateTime firstHour = firstTimestamp.withMinute(0).withSecond(0);
            String previousRow = null;
            LocalDateTime previousTimestamp = null;

            // Write the first row (aligning with the earliest hour, if needed)
            writer.write(currentLine + ",0");
            writer.newLine();
            previousRow = currentLine;
            previousTimestamp = firstHour;

            // Process the rest of the rows
            while ((currentLine = reader.readLine()) != null) {
                currentLine = currentLine.trim();

                // Parse the current timestamp
                String[] fields = currentLine.split(",");
                LocalDateTime currentTimestamp = LocalDateTime.parse(fields[dateTimeIndex], FORMATTER);

                // Align the current timestamp to the nearest hour
                currentTimestamp = currentTimestamp.withMinute(0).withSecond(0);

                // Fill missing hours between the previous timestamp and the current timestamp
                LocalDateTime nextHour = previousTimestamp.plusHours(1);
                while (nextHour.isBefore(currentTimestamp)) {
                    addedCount++;
                    // Generate a holiday row if there's a gap
                    String holidayRow = createHolidayRow(header, nextHour, previousRow);
                    writer.write(holidayRow + ",1"); // Write holiday tick row with 'holiday=1'
                    writer.newLine();
                    nextHour = nextHour.plusHours(1);
                }

                // Write the current row (existing data)
                writer.write(currentLine + ",0");
                writer.newLine();

                // Update the previous row and timestamp
                previousRow = currentLine;
                previousTimestamp = currentTimestamp;
            }

            // No rows should be added after the last timestamp; stop here
        }
        return addedCount;
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