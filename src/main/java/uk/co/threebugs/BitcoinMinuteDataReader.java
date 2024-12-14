package uk.co.threebugs;
// Main Java class to read the Bitcoin minute data file

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

public class BitcoinMinuteDataReader {

    public static void main(String[] args) {
        var reader = new BitcoinMinuteDataReader();
        try {
            var data = reader.readFile(Path.of("data", "btcusd_1-min_data.csv"));
            data.forEach(System.out::println);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

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

                        return new MinuteData(timestamp, open, high, low, close, volume);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Error parsing line: " + e.getMessage());
                    }
                });
    }

    public static class MinuteData {
        private final long timestamp;
        private final double open;
        private final double high;
        private final double low;
        private final double close;
        private final double volume;

        public MinuteData(long timestamp, double open, double high, double low, double close, double volume) {
            this.timestamp = timestamp;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }

        @Override
        public String toString() {
            return "MinuteData{" + "timestamp=" + timestamp + ", open=" + open + ", high=" + high + ", low=" + low + ", close=" + close + ", volume=" + volume + '}';
        }

        // Getters can be added if needed
    }
}
