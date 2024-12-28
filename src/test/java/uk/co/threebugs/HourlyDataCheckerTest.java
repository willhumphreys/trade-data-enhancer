package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HourlyDataCheckerTest {

    private final HourlyDataChecker checker = new HourlyDataChecker();

    @Test
    void ensureHourlyEntries_shouldFillMissingHours(@org.jetbrains.annotations.NotNull @TempDir Path tempDir) throws IOException {
        // Arrange: Prepare input and output file paths
        Path inputFile = tempDir.resolve("input.csv");
        Path outputFile = tempDir.resolve("output.csv");

        // Write input data to inputFile
        // Header: timestamp, Open, High, Low, Close, Volume
        Files.write(inputFile, List.of("timestamp,open,high,low,close,volume", formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),  // 17:00
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 102, 106, 98, 104, 250),  // 18:00
                formatEntry(LocalDateTime.of(2023, 2, 24, 20, 0), 104, 107, 99, 105, 300)   // 20:00 (gap at 19:00)
        ), StandardOpenOption.CREATE);

        // Act: Process the input file to ensure hourly entries
        checker.ensureHourlyEntries(inputFile, outputFile);

        // Read output data
        List<String> outputLines = Files.readAllLines(outputFile);

        // Assert: Validate the content of the output file
        assertThat(outputLines).hasSize(5); // 3 data rows + header + 1 added row for gap
        assertThat(outputLines.getFirst()).isEqualTo("timestamp,open,high,low,close,volume");

        // Extract all DataEntry information into a list for easier validation
        List<DataEntry> outputEntries = toDataEntries(outputLines.subList(1, outputLines.size()));

        // Validate timestamps are consecutive (hourly) and no gaps exist
        LocalDateTime expectedTimestamp = LocalDateTime.of(2023, 2, 24, 17, 0); // Start time
        for (DataEntry entry : outputEntries) {
            assertThat(entry.timestamp()).isEqualTo(expectedTimestamp);
            expectedTimestamp = expectedTimestamp.plusHours(1);
        }

        // Validate missing entries (19:00) have volume = 0 and other fields match the previous row (18:00)
        DataEntry gapEntry = outputEntries.get(2); // 19:00 (third data row)
        assertThat(gapEntry.timestamp()).isEqualTo(LocalDateTime.of(2023, 2, 24, 19, 0));
        assertThat(gapEntry.volume()).isZero();
        assertThat(gapEntry.open()).isEqualTo(102); // Copied from 18:00
        assertThat(gapEntry.high()).isEqualTo(106);
        assertThat(gapEntry.low()).isEqualTo(98);
        assertThat(gapEntry.close()).isEqualTo(104);
    }

    /**
     * Formats an entry as a CSV row using LocalDateTime.
     *
     * @param timestamp LocalDateTime representing the time
     * @param open      open price
     * @param high      high price
     * @param low       low price
     * @param close     close price
     * @param volume    volume traded
     * @return CSV formatted row
     */
    private String formatEntry(LocalDateTime timestamp, double open, double high, double low, double close, double volume) {
        long epochSeconds = timestamp.toEpochSecond(java.time.ZoneOffset.UTC);
        return epochSeconds + "," + open + "," + high + "," + low + "," + close + "," + volume;
    }

    /**
     * Converts a list of CSV lines into a list of DataEntry objects.
     *
     * @param lines CSV lines
     * @return List of DataEntry objects
     */
    private List<DataEntry> toDataEntries(List<String> lines) {
        List<DataEntry> entries = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split(",");
            LocalDateTime timestamp = LocalDateTime.ofEpochSecond(Long.parseLong(parts[0]), 0, java.time.ZoneOffset.UTC);
            double open = Double.parseDouble(parts[1]);
            double high = Double.parseDouble(parts[2]);
            double low = Double.parseDouble(parts[3]);
            double close = Double.parseDouble(parts[4]);
            double volume = Double.parseDouble(parts[5]);
            entries.add(DataEntry.builder().timestamp(timestamp).open(open).high(high).low(low).close(close).volume(volume).build());
        }
        return entries;
    }


    @Test
    void ensureHourlyEntries_shouldHandleNoGaps(@TempDir Path tempDir) throws IOException {
        Path inputFile = tempDir.resolve("input.csv");
        Path outputFile = tempDir.resolve("output.csv");

        Files.write(inputFile, List.of("timestamp,open,high,low,close,volume", formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 102, 106, 98, 104, 250), formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 104, 110, 100, 108, 300)), StandardOpenOption.CREATE);

        checker.ensureHourlyEntries(inputFile, outputFile);

        List<String> outputLines = Files.readAllLines(outputFile);

        assertThat(outputLines).hasSize(4); // No gaps, so only header + original rows
        assertThat(outputLines.getFirst()).isEqualTo("timestamp,open,high,low,close,volume");
    }

    @Test
    void ensureHourlyEntries_shouldHandleEmptyInputFile(@TempDir Path tempDir) throws IOException {
        Path inputFile = tempDir.resolve("input.csv");
        Path outputFile = tempDir.resolve("output.csv");

        Files.write(inputFile, List.of("timestamp,open,high,low,close,volume"), StandardOpenOption.CREATE); // Header only

        checker.ensureHourlyEntries(inputFile, outputFile);

        List<String> outputLines = Files.readAllLines(outputFile);

        assertThat(outputLines).hasSize(1); // Only the header should exist
        assertThat(outputLines.getFirst()).isEqualTo("timestamp,open,high,low,close,volume");
    }

    @Test
    void ensureHourlyEntries_shouldPreserveFieldsDuringGaps(@TempDir Path tempDir) throws IOException {
        Path inputFile = tempDir.resolve("input.csv");
        Path outputFile = tempDir.resolve("output.csv");

        Files.write(inputFile, List.of("timestamp,open,high,low,close,volume", formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200), formatEntry(LocalDateTime.of(2023, 2, 24, 19, 0), 105, 110, 100, 108, 300)), StandardOpenOption.CREATE);

        checker.ensureHourlyEntries(inputFile, outputFile);

        List<String> outputLines = Files.readAllLines(outputFile);

        List<DataEntry> outputEntries = toDataEntries(outputLines.subList(1, outputLines.size()));

        DataEntry filledGapEntry = outputEntries.get(1); // Gap at 18:00

        assertThat(filledGapEntry.open()).isEqualTo(100); // Fields carried over from 17:00
        assertThat(filledGapEntry.high()).isEqualTo(105);
        assertThat(filledGapEntry.low()).isEqualTo(95);
        assertThat(filledGapEntry.close()).isEqualTo(102);
        assertThat(filledGapEntry.volume()).isZero();
    }

    @Test
    void ensureHourlyEntries_shouldPreserveEntriesBetweenHoursWithoutGaps(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path inputFile = tempDir.resolve("input_with_intermediate_entries.csv");
        Path outputFile = tempDir.resolve("output.csv");

        Files.write(inputFile, List.of(
                "timestamp,open,high,low,close,volume",
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 0), 100, 105, 95, 102, 200),  // 17:00
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 15), 102, 106, 96, 103, 210), // 17:15 (intermediate)
                formatEntry(LocalDateTime.of(2023, 2, 24, 17, 30), 104, 108, 97, 105, 220), // 17:30 (intermediate)
                formatEntry(LocalDateTime.of(2023, 2, 24, 18, 0), 105, 110, 100, 107, 250)  // 18:00
        ), StandardOpenOption.CREATE);

        // Act
        checker.ensureHourlyEntries(inputFile, outputFile);

        // Read output
        List<String> outputLines = Files.readAllLines(outputFile);

        // Assert
        assertThat(outputLines).hasSize(5); // Header + original lines (no gaps)
        assertThat(toDataEntries(outputLines.subList(1, outputLines.size())))
                .extracting(DataEntry::timestamp)
                .containsExactly(
                        LocalDateTime.of(2023, 2, 24, 17, 0),  // 17:00
                        LocalDateTime.of(2023, 2, 24, 17, 15), // 17:15
                        LocalDateTime.of(2023, 2, 24, 17, 30), // 17:30
                        LocalDateTime.of(2023, 2, 24, 18, 0)   // 18:00
                );
    }

}