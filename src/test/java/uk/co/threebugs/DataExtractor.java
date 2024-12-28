import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

public class DataExtractor {

    public static void extractDate(Path inputPath, Path outputPath, LocalDate targetDate) throws IOException {
        List<String> filteredLines = Files.readAllLines(inputPath).stream()
                .filter(line -> {
                    try {
                        String[] columns = line.split(",");
                        if (columns[0].equals("Timestamp")) return true; // Keep header row
                        long timestamp = (long) Double.parseDouble(columns[0]);
                        LocalDate rowDate = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC).toLocalDate();
                        return rowDate.equals(targetDate);
                    } catch (Exception e) {
                        return false; // Skip malformed rows
                    }
                })
                .collect(Collectors.toList());
        
        Files.write(outputPath, filteredLines);
    }

    public static void main(String[] args) throws IOException {
        Path inputFile = Path.of("data/output/2_decimal_shifted_btcusd_1-min_data.csv");
        Path outputFile = Path.of("data/output/test_data_2012-11-04.csv");
        LocalDate targetDate = LocalDate.of(2012, 11, 4);

        extractDate(inputFile, outputFile, targetDate);

        System.out.println("Data for " + targetDate + " has been extracted to: " + outputFile);
    }
}