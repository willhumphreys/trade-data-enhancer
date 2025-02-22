package uk.co.threebugs;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class HourlyFileGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    public void testGenerateHourlyFileFromMinuteFile() throws IOException {
        // Create a temporary minute data file with a header and sample rows.
        Path minuteDataPath = tempDir.resolve("minute_data.csv");
        List<String> minuteDataLines = List.of(
                "Timestamp,Open,High,Low,Close,Volume",
                "3600,1.0,1.5,0.9,1.2,100",
                "3659,1.2,1.6,1.0,1.1,50",
                "7200,2.0,2.5,1.9,2.2,100",
                "7300,2.2,2.6,2.0,2.1,50"
        );
        Files.write(minuteDataPath, minuteDataLines);

        // Create an output file for the hourly data.
        Path hourlyDataPath = tempDir.resolve("hourly_data.csv");

        // Instantiate the generator and execute the file generation.
        HourlyFileGenerator generator = new HourlyFileGenerator();
        generator.generateHourlyFileFromMinuteFile(minuteDataPath, hourlyDataPath);

        // Read the generated hourly data file.
        List<String> actualLines = Files.readAllLines(hourlyDataPath);

        // Build the expected output.
        // There are two hourly rows:
        // For 3600 bucket: open = 1.0, close = 1.1, high = 1.6, low = 0.9, volume = 150.
        // For 7200 bucket: open = 2.0, close = 2.1, high = 2.6, low = 1.9, volume = 150.
        // Note: The HourlyData.toCsvRow() formats the timestamp with one decimal
        // and the other numbers as indicated by the formatting string.
        List<String> expectedLines = List.of(
                "Timestamp,Open,High,Low,Close,Volume",
                "3600.0,1.00,1.60,0.90,1.10,150.00",
                "7200.0,2.00,2.60,1.90,2.10,150.00"
        );

        // Validate that the file has the expected content.
        Assertions.assertThat(actualLines)
                .hasSize(3)
                .containsExactlyElementsOf(expectedLines);
    }
}