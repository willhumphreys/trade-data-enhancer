package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class DuplicateRemover {

    /**
     * Removes duplicate timestamps from the sorted file.
     * Only the first occurrence of each timestamp is kept.
     *
     * @param sortedFile       Path to the input, already sorted file.
     * @param deduplicatedFile Path to the output deduplicated file.
     * @throws IOException If file operations fail.
     */
    public void removeDuplicates(Path sortedFile, Path deduplicatedFile) throws IOException {
        // Read all lines from the file
        List<String> lines = Files.readAllLines(sortedFile);

        if (lines.isEmpty()) {
            log.warn("The sorted file is empty. No duplicates to remove.");
            Files.write(deduplicatedFile, lines);
            return;
        }

        // Separate header and data rows
        String header = lines.get(0); // Keep the header
        List<String> dataRows = lines.subList(1, lines.size());

        // Deduplicate based on the first column (timestamp)
        HashSet<String> seenTimestamps = new HashSet<>();
        int duplicateCount = 0;

        StringBuilder deduplicatedData = new StringBuilder();
        deduplicatedData.append(header).append(System.lineSeparator());

        for (String row : dataRows) {
            String[] columns = row.split(",");
            String timestamp = columns[0]; // Assuming the first column is the timestamp
            if (seenTimestamps.contains(timestamp)) {
                duplicateCount++;
            } else {
                deduplicatedData.append(row).append(System.lineSeparator());
                seenTimestamps.add(timestamp);
            }
        }

        // Write the deduplicated data to a new file
        Files.writeString(deduplicatedFile, deduplicatedData.toString());

        // Log the total number of duplicates removed
        log.info("Duplicates removed: {}", duplicateCount);
    }
}