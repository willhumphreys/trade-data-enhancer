package uk.co.threebugs;

import java.io.*;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

public class MissingHourAdder {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    /**
     * Adds missing hourly rows to the data.
     *
     * @param inputPath  Path to the input file (CSV with the last completed step).
     * @param outputPath Path to write the completed file with added rows.
     * @throws IOException If reading or writing fails.
     */
    public void addMissingHours(Path inputPath, Path outputPath) throws IOException {
        List<String> rows = new ArrayList<>(); // Store input rows
        List<String> output = new ArrayList<>(); // Store output rows

        // Read the input file
        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath.toFile()))) {
            String header = reader.readLine();
            if (header == null) throw new IOException("Input file is empty.");
            rows.add(header); // Preserve the header
            String line;
            while ((line = reader.readLine()) != null) {
                rows.add(line);
            }
        }

        // Determine column positions
        String headerRow = rows.getFirst();
        String[] headers = headerRow.split(",");
        int dateTimeIndex = Arrays.asList(headers).indexOf("dateTime");
        int nameIndex = Arrays.asList(headers).indexOf("name");

        if (dateTimeIndex == -1 || nameIndex == -1) {
            throw new IllegalStateException("Missing required columns: 'dateTime' or 'name'");
        }

        // Parse existing rows and check for missing hours
        TreeMap<LocalDateTime, String> rowMap = new TreeMap<>();
        for (int i = 1; i < rows.size(); i++) { // Skip the header
            String[] fields = rows.get(i).split(",");
            LocalDateTime dateTime = LocalDateTime.parse(fields[dateTimeIndex], FORMATTER);
            rowMap.put(dateTime, rows.get(i)); // Map rows by their dateTime
        }

        // Detect gaps between hourly timestamps
        LocalDateTime startTime = rowMap.firstKey().withMinute(0).withSecond(0); // Align to the hour
        LocalDateTime endTime = rowMap.lastKey().withMinute(0).withSecond(0);

        output.add(headerRow + ",holiday"); // Add the holiday column to the header
        for (LocalDateTime current = startTime; !current.isAfter(endTime); current = current.plusHours(1)) {
            if (rowMap.containsKey(current)) {
                // Write the existing row and set holiday to 0
                String existingRow = rowMap.get(current);
                output.add(existingRow + ",0"); // holiday = 0 for existing rows
            } else {
                // Add a missing hourly row
                String newRow = createMissingRow(headers, current, rowMap.firstEntry().getValue(), nameIndex);
                output.add(newRow + ",1"); // holiday = 1 for missing rows
            }
        }

        // Write the output to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath.toFile()))) {
            for (String line : output) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    /**
     * Creates a missing hourly row with most fields set to -1 and the given hourly timestamp.
     *
     * @param headers      The headers of the CSV file.
     * @param dateTime     The missing hour timestamp.
     * @param referenceRow A row to match the structure for name or other fields (if needed).
     * @param nameIndex    The index of the 'name' column.
     * @return A new row as a String.
     */
    private String createMissingRow(String[] headers, LocalDateTime dateTime, String referenceRow, int nameIndex) {
        StringBuilder newRow = new StringBuilder();

        for (int i = 0; i < headers.length; i++) {
            if (i == nameIndex) {
                // Set the name field using the reference row
                String[] referenceFields = referenceRow.split(",");
                newRow.append(referenceFields[nameIndex]); // Use the existing 'name'
            } else if ("dateTime".equals(headers[i])) {
                // Set the dateTime field to the missing hour
                newRow.append(dateTime.format(FORMATTER));
            } else if ("mapTime".equals(headers[i])) {
                // Set mapTime to the missing hour
                newRow.append(dateTime.format(FORMATTER));
            } else {
                // Set all other fields to -1
                newRow.append("-1");
            }

            if (i < headers.length - 1) {
                newRow.append(","); // Add a comma between fields
            }
        }

        return newRow.toString();
    }
}