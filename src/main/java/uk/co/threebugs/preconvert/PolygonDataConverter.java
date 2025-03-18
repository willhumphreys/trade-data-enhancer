package uk.co.threebugs.preconvert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PolygonDataConverter implements SourceDataConverter {
    @Override
    public void convert(Path input, Path output) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(input);
             BufferedWriter writer = Files.newBufferedWriter(output)) {

            // Write header for output file
            writer.write("Timestamp,Open,High,Low,Close,Volume");
            writer.newLine();

            // Skip header line in input file
            String line = reader.readLine();

            // Process each line
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue; // Skip empty lines
                }

                String[] parts = line.split(",");
                if (parts.length < 8) {
                    continue; // Skip malformed lines
                }

                // Extract values from input format
                double open = Double.parseDouble(parts[0]);
                double high = Double.parseDouble(parts[1]);
                double low = Double.parseDouble(parts[2]);
                double close = Double.parseDouble(parts[3]);
                double volume = Double.parseDouble(parts[4]);

                // Convert timestamp from milliseconds to seconds
                long timestampMs = Long.parseLong(parts[6]);
                double timestampSec = timestampMs / 1000.0;

                // Format output line
                String outputLine = String.format("%.1f,%.2f,%.2f,%.2f,%.2f,%.2f",
                        timestampSec, open, high, low, close, volume);

                writer.write(outputLine);
                writer.newLine();
            }
        }
    }
}