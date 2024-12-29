package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HourlyDataCheckerTest {

    private final HourlyDataChecker checker = new HourlyDataChecker();

    @Test
    void ensureHourlyEntries_shouldFillMissingHoursUsingHourlyData(@TempDir Path tempDir) throws IOException {
        // Arrange: Set up input minute and hourly data files
        Path minuteFile = tempDir.resolve("minute_data.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");
        Path outputFile = tempDir.resolve("output.csv");

        // Minute data: missing 19:00
        Files.write(minuteFile, List.of("timestamp,open,high,low,close,volume", // Header
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 102, 106, 98, 104, 250), // Continuation
                formatEntry(LocalDateTime.of(2023, 2, 24, 20, 0), 108, 112, 102, 110, 300)));

        // Hourly data provides open values for missing rows
        Files.write(hourlyFile, List.of("timestamp,open,high,low,close,volume", // Header
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 102, 106, 98, 104, 250), formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 105, 110, 100, 108, 0),  // Provided hourly row
                formatEntry(LocalDateTime.of(2023, 2, 24, 20, 0), 108, 112, 102, 110, 300)));

        // Act: Run the checker to fill gaps
        checker.ensureHourlyEntries(minuteFile, hourlyFile, outputFile);

        // Assert: Read output and validate content
        List<String> outputLines = Files.readAllLines(outputFile);
        assertThat(outputLines).hasSize(5); // Header + 4 rows (no gaps)
        assertThat(outputLines.getFirst()).isEqualTo("timestamp,open,high,low,close,volume");

        // Convert output rows to DataEntry for validation
        List<DataEntry> entries = toDataEntries(outputLines.subList(1, outputLines.size()));

        // Validate timestamps and field values
        LocalDateTime[] expectedTimestamps = {LocalDateTime.of(2023, 2, 24, 17, 0), LocalDateTime.of(2023, 2, 24, 18, 0), LocalDateTime.of(2023, 2, 24, 19, 0), LocalDateTime.of(2023, 2, 24, 20, 0)};

        for (int i = 0; i < entries.size(); i++) {
            assertThat(entries.get(i).timestamp()).isEqualTo(expectedTimestamps[i]);
        }

        // Check the filled 19:00 row (values for high, low, close from 18:00)
        DataEntry filledEntry = entries.get(2); // 19:00
        assertThat(filledEntry.open()).isEqualTo(105); // From hourly file
        assertThat(filledEntry.high()).isEqualTo(106); // From 18:00 row
        assertThat(filledEntry.low()).isEqualTo(98);
        assertThat(filledEntry.close()).isEqualTo(104);
    }

    @Test
    void ensureHourlyEntries_shouldHandleNoHourlyDataForMissingRows(@TempDir Path tempDir) throws IOException {
        // Arrange: Create minute and empty hourly files
        Path minuteFile = tempDir.resolve("minute_data.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");
        Path outputFile = tempDir.resolve("output.csv");

        // Minute data with gap at 18:00
        Files.write(minuteFile, List.of("timestamp,open,high,low,close,volume", // Header
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 104, 110, 100, 108, 300)));

        // Empty hourly data file
        Files.write(hourlyFile, List.of("timestamp,open,high,low,close,volume")); // Header only

        // Act: Run the checker
        checker.ensureHourlyEntries(minuteFile, hourlyFile, outputFile);

        // Assert
        List<String> outputLines = Files.readAllLines(outputFile);
        assertThat(outputLines).hasSize(3); // Header + 2 rows (17:00 and 19:00 only)
        assertThat(outputLines.getFirst()).isEqualTo("timestamp,open,high,low,close,volume");

        // Validate the rows in output
        List<DataEntry> rows = toDataEntries(outputLines.subList(1, outputLines.size()));
        assertThat(rows.get(0).timestamp()).isEqualTo(LocalDateTime.of(2023, 2, 24, 17, 0)); // 17:00 row
        assertThat(rows.get(1).timestamp()).isEqualTo(LocalDateTime.of(2023, 2, 24, 19, 0)); // 19:00 row
    }

    @Test
    void ensureHourlyEntries_shouldFillGapsWhenMinuteDataStartsOffHour(@TempDir Path tempDir) throws IOException {
        // Arrange: Create minute and hourly files
        Path minuteFile = tempDir.resolve("minute_data.csv");
        Path hourlyFile = tempDir.resolve("hourly_data.csv");
        Path outputFile = tempDir.resolve("output.csv");

        // Minute data starts at 17:05 (not aligned to 0) and has a gap at 18:00
        Files.write(minuteFile, List.of("timestamp,open,high,low,close,volume", // Header
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 5), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 104, 110, 100, 108, 300)));

        // Hourly data includes 17:00, 18:00, and 19:00 entries
        Files.write(hourlyFile, List.of("timestamp,open,high,low,close,volume", // Header
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 108, 250), formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 104, 110, 100, 108, 300)));

        // Act: Run the checker
        checker.ensureHourlyEntries(minuteFile, hourlyFile, outputFile);

        // Assert
        List<String> outputLines = Files.readAllLines(outputFile);
        assertThat(outputLines).hasSize(4); // Header + 3 rows (17:05, 18:00, 19:00)
        assertThat(outputLines.getFirst()).isEqualTo("timestamp,open,high,low,close,volume");

        // Validate rows
        List<DataEntry> rows = toDataEntries(outputLines.subList(1, outputLines.size()));
        assertThat(rows.get(0).timestamp()).isEqualTo(LocalDateTime.of(2023, 2, 24, 17, 5)); // Original 17:05
        assertThat(rows.get(1).timestamp()).isEqualTo(LocalDateTime.of(2023, 2, 24, 18, 0)); // Filled 18:00
        assertThat(rows.get(1).open()).isEqualTo(105); // From hourly data (18:00)
        assertThat(rows.get(2).timestamp()).isEqualTo(LocalDateTime.of(2023, 2, 24, 19, 0)); // Original 19:00
    }

    // Utility methods remain the same as in the previous tests
    private String formatEntry(LocalDateTime timestamp, long open, long high, long low, long close, double volume) {
        long epochSecond = timestamp.toEpochSecond(ZoneOffset.UTC);
        return String.format("%d,%d,%d,%d,%d,%f", epochSecond, open, high, low, close, volume);
    }

    private List<DataEntry> toDataEntries(List<String> lines) {
        List<DataEntry> entries = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split(",");
            LocalDateTime timestamp = LocalDateTime.ofEpochSecond((long) Double.parseDouble(parts[0]), 0, ZoneOffset.UTC);
            entries.add(new DataEntry(timestamp, Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]), Long.parseLong(parts[4]), Double.parseDouble(parts[5])));
        }
        return entries;
    }
}