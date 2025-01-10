package preconvert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class BackTestMarket2024DataFormatConverter {

    // Define input and output date formatting
    private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MM/yyyy;HH:mm:ss");

    public static void convertFormat(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath);
             BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            // Write header for the output file
            writer.write("Timestamp,Open,High,Low,Close,Volume\n");

            String line;

            // Read the input file line by line
            while ((line = reader.readLine()) != null) {
                // Ignore empty lines
                if (line.isBlank()) {
                    continue;
                }

                // Split the input line by `;` delimiter
                String[] parts = line.split(";");
                if (parts.length < 6) {
                    throw new IllegalArgumentException("Invalid input format on line: " + line);
                }

                if (line.startsWith("Time")) {
                    continue;
                }

                // Parse date and time
                LocalDateTime dateTime = LocalDateTime.parse(parts[0] + ";" + parts[1], INPUT_DATE_FORMAT);
                long timestamp = dateTime.toEpochSecond(ZoneOffset.UTC);

                // Parse Open, High, Low, Close, and Volume
                double open = Double.parseDouble(parts[2]);
                double high = Double.parseDouble(parts[3]);
                double low = Double.parseDouble(parts[4]);
                double close = Double.parseDouble(parts[5]);
                double volume = Double.parseDouble(parts[6]); // Assume it's a floating-point value

                // Write the formatted line to the output file
                writer.write(String.format("%d.0,%.2f,%.2f,%.2f,%.2f,%.1f\n",
                        timestamp, open, high, low, close, volume));
            }
        }
    }

    public static void main(String[] args) {
        // Example usage
        Path inputPath = Path.of("data/input/xauusd-1m.csv");
        Path outputPath = Path.of("data/input/xauusd-1mF.csv");

        try {
            convertFormat(inputPath, outputPath);
            System.out.println("File converted successfully: " + outputPath);
        } catch (IOException e) {
            System.err.println("An error occurred during file conversion: " + e.getMessage());
        }
    }
}