package uk.co.threebugs;
// Main Java class to read the Bitcoin minute data file

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

public class BitcoinMinuteDataReader {

    public Stream<MinuteData> readFile(Path filePath) throws IOException {
        return new BufferedReader(new FileReader(filePath.toFile())).lines().skip(1) // Skip the header
                .map(line -> {
                    var parts = line.split(",");
                    if (parts.length != 6) {
                        throw new IllegalArgumentException("Error parsing line: " + line);
                    }
                    try {
                        var timestamp = (long) Double.parseDouble(parts[0]);
                        var open = Double.parseDouble(parts[1]);
                        var high = Double.parseDouble(parts[2]);
                        var low = Double.parseDouble(parts[3]);
                        var close = Double.parseDouble(parts[4]);
                        var volume = Double.parseDouble(parts[5]);

                        return MinuteData.builder().timestamp(timestamp).open(open).high(high).low(low).close(close).volume(volume).build();

                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Error parsing line: " + e.getMessage());
                    }
                });
    }

}
