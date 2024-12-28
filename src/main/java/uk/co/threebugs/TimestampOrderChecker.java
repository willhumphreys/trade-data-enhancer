package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TimestampOrderChecker {

    public void checkTimestampOrder(Path filePath) throws IOException {
        // Read all lines from the file
        var lines = Files.readAllLines(filePath);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("The input file is empty!");
        }

        // Skip the header row
        long previousTimestamp = -1; // Initially set to an invalid timestamp
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] columns = line.split(",");
            long currentTimestamp = (long) Double.parseDouble(columns[0]);

            // Check that the current timestamp is in order
            if (previousTimestamp != -1 && currentTimestamp < previousTimestamp) {
                throw new IllegalStateException(String.format(
                        "Timestamps are out of order at line %d! Previous: %d, Current: %d",
                        i + 1, previousTimestamp, currentTimestamp
                ));
            }

            previousTimestamp = currentTimestamp;
        }
    }
}