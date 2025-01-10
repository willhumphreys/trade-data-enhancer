package uk.co.threebugs;

import java.io.*;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MissingHourAdder {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    public long addMissingHours(Path inputPath, Path outputPath) throws IOException {
        long addedCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath.toFile()));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath.toFile()))) {

            // Read and write the header
            String header = reader.readLine();
            if (header == null) throw new IOException("Input file is empty.");
            writer.write(header + ",holiday");
            writer.newLine();

            // Pre-calculate and store indices based on the header
            String[] headers = header.split(",");
            int indexDateTime = getIndex(headers, "dateTime");
            int indexName = getIndex(headers, "name");
            int indexNewHour = getIndex(headers, "newHour");
            int indexMapTime = getIndex(headers, "mapTime");

            String previousRow = null;
            LocalDateTime previousTimestamp = null;

            String currentRow;
            while ((currentRow = reader.readLine()) != null) {
                // Parse the current timestamp
                String[] currentFields = currentRow.split(",");
                LocalDateTime currentTimestamp = LocalDateTime.parse(currentFields[indexDateTime], FORMATTER);

                // Add missing rows for gaps between previous and current row timestamps
                if (previousRow != null && previousTimestamp != null) {
                    LocalDateTime nextExpectedTimestamp = previousTimestamp.plusHours(1).withMinute(0); // Set minutes to 0
                    while (nextExpectedTimestamp.isBefore(currentTimestamp)) {
                        // Write a generated row for each missing hour
                        writer.write(generateRow(headers, previousRow, nextExpectedTimestamp, indexDateTime, indexName, indexNewHour, indexMapTime) + ",1");
                        writer.newLine();
                        addedCount++;
                        nextExpectedTimestamp = nextExpectedTimestamp.plusHours(1);
                    }
                }

                // Write the current row and mark it as not missing (holiday = 0)
                writer.write(currentRow + ",0");
                writer.newLine();

                // Update the previous row and timestamp
                previousRow = currentRow;
                previousTimestamp = currentTimestamp;
            }
        }

        return addedCount;
    }

    private String generateRow(String[] headers, String exampleRow, LocalDateTime timestamp, int indexDateTime, int indexName, int indexNewHour, int indexMapTime) {
        StringBuilder row = new StringBuilder();

        for (int i = 0; i < headers.length; i++) {
            if (i == indexDateTime) {
                row.append(timestamp.format(FORMATTER)); // Insert the missing hour's timestamp
            } else if (i == indexName) {
                row.append(getColumnValue(exampleRow, i)); // Use the instrument name from the example row
            } else if (i == indexNewHour) {
                row.append("true"); // Mark this row as generated
            } else if (i == indexMapTime) {
                row.append(timestamp.format(FORMATTER)); // Insert the missing hour's timestamp
            } else {
                row.append("-1"); // Use default value for other fields not specified
            }
            row.append(",");
        }

        // Remove trailing comma
        if (row.charAt(row.length() - 1) == ',') {
            row.deleteCharAt(row.length() - 1);
        }

        return row.toString();
    }

    private int getIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equals(columnName.trim())) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    private String getColumnValue(String row, int index) {
        String[] columns = row.split(",");
        return columns[index];
    }
}