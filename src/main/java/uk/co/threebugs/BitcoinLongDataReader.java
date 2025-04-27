package uk.co.threebugs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

public class BitcoinLongDataReader {

    /**
     * Reads a file where numerical values are stored as decimal-shifted `long` values.
     * Assumes that the file follows the format:
     * Timestamp,Open,High,Low,Close,Volume
     *
     * @param filePath The path to the file to be read.
     * @return A Stream of ShiftedMinuteData objects.
     * @throws IOException              If the file cannot be read.
     * @throws IllegalArgumentException If there are issues parsing lines.
     */
    public Stream<ShiftedData> readFile(Path filePath) throws IOException {
        return new BufferedReader(new FileReader(filePath.toFile()))
                .lines()
                .skip(1) // Skip the header
                .map(line -> {
                    var parts = line.split(",");
                    if (parts.length != 6) {
                        throw new IllegalArgumentException("Error parsing line: Incorrect column count: " + line);
                    }
                    try {
                        var timestamp = (long) Double.parseDouble(parts[0]);
                        var open = Long.parseLong(parts[1]);
                        var high = Long.parseLong(parts[2]);
                        var low = Long.parseLong(parts[3]);
                        var close = Long.parseLong(parts[4]);
                        var volume = Double.parseDouble(parts[5]);

                        return ShiftedData.builder()
                                .timestamp(timestamp)
                                .open(open)
                                .high(high)
                                .low(low)
                                .close(close)
                                .volume(volume)
                                .build();

                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Error parsing line: " + line, e);
                    }
                });
    }
}