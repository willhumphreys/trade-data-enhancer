package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DataIntegrityCheckerTest {

    private final DataIntegrityChecker checker = new DataIntegrityChecker();

    @Test
    void validateData_shouldDetectGapForMissingHourlyEntryInHourlyFile(@TempDir Path tempDir) throws IOException {
        // Arrange: Create a file with a gap that corresponds to an entry in the hourly data file.
        Path minuteFile = tempDir.resolve("minute_with_gap.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");

        // Minute data file has a gap at 18:00
        Files.write(minuteFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 100, 112, 350)
        ), StandardOpenOption.CREATE);

        // Hourly data includes 18:00
        Files.write(hourlyFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250)
        ), StandardOpenOption.CREATE);

        // Act and Assert
        IllegalStateException exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> {
            checker.validateDataIntegrity(minuteFile, hourlyFile);
        });

        // Verifying exception message
        assertThat(exception.getMessage()).isEqualTo("Error: Gap detected for timestamp where hourly data exists! Expected: 2023-02-24T18:00, Found: 2023-02-24T19:00.");
    }

    @Test
    void validateData_shouldAllowGapWhenNoHourlyEntryExistsInHourlyFile(@TempDir Path tempDir) throws IOException {
        // Arrange: Create a file with a gap that doesn't correspond to any entry in the hourly data file.
        Path minuteFile = tempDir.resolve("minute_with_allowed_gap.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");

        // Minute data file has a gap at 18:00
        Files.write(minuteFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 1), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 100, 112, 350)
        ), StandardOpenOption.CREATE);

        // Hourly data does not include 18:00
        Files.write(hourlyFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 100, 112, 350)
        ), StandardOpenOption.CREATE);

        // Act
        String result = checker.validateDataIntegrity(minuteFile, hourlyFile);

        // Assert
        assertThat(result).isEqualTo("No issues found."); // Gaps allowed because hourly file doesn't have the missing entry.
    }

    @Test
    void validateData_shouldDetectLackOfIntermediateData(@TempDir Path tempDir) throws IOException {
        // Arrange: Create a file with only hourly entries and no intermediate data.
        Path minuteFile = tempDir.resolve("only_hourly_entries.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");

        // Minute data has only hourly entries
        Files.write(minuteFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250),
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 350)
        ), StandardOpenOption.CREATE);

        // Hourly data matches the minute data
        Files.write(hourlyFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250),
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 350)
        ), StandardOpenOption.CREATE);

        // Act
        String result = checker.validateDataIntegrity(minuteFile, hourlyFile);

        // Assert
        assertThat(result).isEqualTo("Error: No intermediate (non-hourly) data entries found in the file.");
    }

    @Test
    void validateData_shouldPassWithIntermediateAndCompleteHourlyEntries(@TempDir Path tempDir) throws IOException {
        // Arrange: Create a correct file with intermediate data and no gaps in hourly entries.
        Path minuteFile = tempDir.resolve("valid_minute_data.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");

        // Minute data includes intermediate entries
        Files.write(minuteFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 30), 102, 106, 96, 103, 210), // Intermediate entry
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250), // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 45), 107, 111, 101, 108, 260), // Intermediate entry
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 350)  // Hourly valid
        ), StandardOpenOption.CREATE);

        // Hourly data matches the minute data
        Files.write(hourlyFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250),
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 350)
        ), StandardOpenOption.CREATE);

        // Act
        String result = checker.validateDataIntegrity(minuteFile, hourlyFile);

        // Assert
        assertThat(result).isEqualTo("No issues found."); // Valid case, no errors expected.
    }

    /**
     * Helper method to generate CSV entries for testing.
     */
    private String formatEntry(LocalDateTime timestamp, double open, double high, double low, double close, double volume) {
        long epochSeconds = timestamp.toEpochSecond(ZoneOffset.UTC);
        return epochSeconds + "," + open + "," + high + "," + low + "," + close + "," + volume;
    }
}