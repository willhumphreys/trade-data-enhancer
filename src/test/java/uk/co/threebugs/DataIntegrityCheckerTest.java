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
    void checkForNoGaps_shouldReturnTrueForDataWithInBetweenEntries(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("data_with_in_between_entries.csv");

        Files.write(file, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),  // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 30), 102, 106, 96, 103, 210), // Extra data (shouldn't affect)
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250), // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 45), 107, 111, 101, 108, 260), // More extra data (shouldn't affect)
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 300)  // Hourly valid
        ), StandardOpenOption.CREATE);

        String noGaps = checker.validateDataIntegrity(file);

        assertThat(noGaps).isEqualTo("No issues found."); // No gaps, hourly entries are intact.
    }

    @Test
    void checkForNoGaps_shouldReturnFalseIfHourlyEntryIsMissingWithInBetweenData(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("data_with_missing_hourly_entry_and_in_between_data.csv");

        Files.write(file, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),  // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 45), 102, 106, 96, 103, 210), // Extra data
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 300)  // Hourly valid
                // Missing 2023-02-24 18:00 entry
        ), StandardOpenOption.CREATE);

        String noGaps = checker.validateDataIntegrity(file);

        assertThat(noGaps).isEqualTo("Error: Gap detected! Expected: 2023-02-24T18:00, Found: 2023-02-24T19:00."); // Gap detected at 18:00.
    }

    @Test
    void checkForNoGaps_shouldReturnTrueWhenOnlyHourlyDataExists(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("only_hourly_data.csv");

        Files.write(file, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),  // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250), // Hourly valid
                formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 110, 115, 105, 112, 300)  // Hourly valid
        ), StandardOpenOption.CREATE);

        String noGaps = checker.validateDataIntegrity(file);

        assertThat(noGaps).isEqualTo("Error: No intermediate (non-hourly) data entries found in the file."); // No gaps, only hourly data is present.
    }

    /**
     * Helper method to generate CSV entries for testing.
     */
    private String formatEntry(LocalDateTime timestamp, double open, double high, double low, double close, double volume) {
        long epochSeconds = timestamp.toEpochSecond(ZoneOffset.UTC);
        return epochSeconds + "," + open + "," + high + "," + low + "," + close + "," + volume;
    }
}