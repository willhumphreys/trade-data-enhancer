package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class FileSorter {

    /**
     * Sorts a CSV file by the first column (assumed to be a timestamp).
     *
     * @param inputPath  The input file path.
     * @param outputPath The output file path for the sorted file.
     * @throws IOException If file operations fail.
     */
    public void sortFileByTimestamp(Path inputPath, Path outputPath) throws IOException {
        // Read all lines from the file
        List<String> lines = Files.readAllLines(inputPath);

        if (lines.isEmpty()) {
            throw new IllegalArgumentException("The input file is empty!");
        }

        // Separate header and data rows
        String header = lines.get(0);
        List<String> dataRows = new ArrayList<>(lines.subList(1, lines.size()));

        // Sort data rows based on the first column (timestamp)
        dataRows.sort(Comparator.comparing(row -> {
            String[] columns = row.split(",");
            return Double.parseDouble(columns[0]); // Assuming first column is the timestamp
        }));

        // Combine header and sorted rows
        List<String> sortedLines = new ArrayList<>();
        sortedLines.add(header);
        sortedLines.addAll(dataRows);

        // Write sorted rows to the output file
        Files.write(outputPath, sortedLines);
    }
}