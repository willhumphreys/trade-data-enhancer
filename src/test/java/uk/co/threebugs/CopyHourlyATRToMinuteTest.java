package uk.co.threebugs;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CopyHourlyATRToMinuteTest {

    @TempDir
    Path tempDir;

    @Test
    public void testCopyHourlyATRToMinute() throws IOException {
        // Create sample hourly data file with ATR values.
        // Format: Timestamp,Open,High,Low,Close,Volume,ATR
        Path hourlyDataFile = tempDir.resolve("hourly_data.csv");
        List<String> hourlyLines = List.of(
                "Timestamp,Open,High,Low,Close,Volume,ATR",
                "3600,1.0,1.5,0.9,1.2,100,250",
                "7200,2.0,2.5,1.9,2.2,150,350"
        );
        Files.write(hourlyDataFile, hourlyLines);

        // Create sample minute data file.
        // Format: Timestamp,open,high,low,close,volume
        Path minuteDataFile = tempDir.resolve("minute_data.csv");
        List<String> minuteLines = List.of(
                "Timestamp,open,high,low,close,volume",
                "3610,10,15,5,12,200",
                "3650,11,16,6,13,210",
                "7300,20,25,15,22,300",
                "8000,21,26,16,23,310"
        );
        Files.write(minuteDataFile, minuteLines);

        // Define the output file path.
        Path atrOutputFile = tempDir.resolve("updated_minute_data.csv");

        // Invoke the method under test.
        CopyHourlyATRToMinute.copyHourlyATRToMinute(minuteDataFile, hourlyDataFile, atrOutputFile);

        // Read the generated output file.
        List<String> outputLines = Files.readAllLines(atrOutputFile);

        // Based on the association logic:
        // For a minute falling in the 3600 bucket -> ATR = 250.
        // For a minute falling in the 7200 bucket -> ATR = 350.
        // Expected output: each minute row appended with the ATR value.
        List<String> expectedLines = List.of(
                "Timestamp,open,high,low,close,volume,atr",
                "3610,10,15,5,12,200.0,250",
                "3650,11,16,6,13,210.0,250",
                "7300,20,25,15,22,300.0,350",
                "8000,21,26,16,23,310.0,350"
        );

        // Validate that the output file content is as expected.
        Assertions.assertThat(outputLines)
                .hasSize(expectedLines.size())
                .containsExactlyElementsOf(expectedLines);
    }
}