package uk.co.threebugs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class HybridScalingAppenderIntegrationTest {

    private HybridScalingAppender dailyAppender;
    private List<ShiftedData> sampleData;

    @BeforeEach
    void setUp() {
        // Using realistic ATR periods and alpha
        dailyAppender = new HybridScalingAppender(5, 14, 0.5, TimeFrame.DAILY);

        // Sample data for testing - needs enough entries for ATR calculation
        sampleData = Arrays.asList(
                new ShiftedData(1678886400, 10000, 10500, 9800, 10200, 10.5), // 2023-03-15 13:00:00
                new ShiftedData(1678886460, 10200, 10300, 10100, 10250, 12.0), // 2023-03-15 13:01:00
                new ShiftedData(1678886520, 10250, 10400, 10150, 10350, 11.2), // 2023-03-15 13:02:00
                new ShiftedData(1678886580, 10350, 10600, 10300, 10500, 15.5), // 2023-03-15 13:03:00
                new ShiftedData(1678886640, 10500, 10550, 10400, 10450, 9.8),  // 2023-03-15 13:04:00
                new ShiftedData(1678886700, 10450, 10700, 10400, 10650, 18.1), // 2023-03-15 13:05:00
                new ShiftedData(1678886760, 10650, 10800, 10600, 10750, 14.3), // 2023-03-15 13:06:00
                new ShiftedData(1678886820, 10750, 10900, 10700, 10850, 20.0), // 2023-03-15 13:07:00
                new ShiftedData(1678886880, 10850, 11000, 10800, 10950, 16.7), // 2023-03-15 13:08:00
                new ShiftedData(1678886940, 10950, 11100, 10900, 11050, 22.3), // 2023-03-15 13:09:00
                new ShiftedData(1678887000, 11050, 11200, 11000, 11150, 19.5), // 2023-03-15 13:10:00
                new ShiftedData(1678887060, 11150, 11250, 11100, 11200, 13.8), // 2023-03-15 13:11:00
                new ShiftedData(1678887120, 11200, 11300, 11150, 11250, 17.9), // 2023-03-15 13:12:00
                new ShiftedData(1678887180, 11250, 11400, 11200, 11350, 21.1)  // 2023-03-15 13:13:00
        );
    }

    @Test
    void integrationTest_shouldAppendAndWriteFactors(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path outputFile = tempDir.resolve("integration_output.csv");
        Stream<ShiftedData> dataStream = sampleData.stream();

        // Act
        // 1. Append factors
        Stream<HybridScalingAppender.ShiftedDataWithHybridScaling> resultStream = dailyAppender.appendScalingFactors(dataStream);
        // 2. Write to file
        dailyAppender.writeStreamToFile(resultStream, outputFile);

        // Assert
        assertThat(Files.exists(outputFile)).isTrue();
        List<String> lines = Files.readAllLines(outputFile);

        // Check header
        assertThat(lines).isNotEmpty();
        assertThat(lines.get(0)).isEqualTo("Timestamp,Open,High,Low,Close,Volume,HybridScalingFactor");

        // Check number of lines (header + data)
        assertThat(lines).hasSize(sampleData.size() + 1);

        // Check content of a specific line (e.g., the last data line, corresponding to sampleData index 13)
        String lastDataLine = lines.get(sampleData.size()); // Index 14 in the file (lines list)
        String[] parts = lastDataLine.split(",");
        assertThat(parts).hasSize(7);

        ShiftedData originalData = sampleData.get(13);
        assertThat(Long.parseLong(parts[0])).isEqualTo(originalData.timestamp());
        assertThat(Long.parseLong(parts[1])).isEqualTo(originalData.open());
        assertThat(Long.parseLong(parts[2])).isEqualTo(originalData.high());
        assertThat(Long.parseLong(parts[3])).isEqualTo(originalData.low());
        assertThat(Long.parseLong(parts[4])).isEqualTo(originalData.close());
        // Compare volume with tolerance due to double formatting
        assertThat(Double.parseDouble(parts[5])).isCloseTo(originalData.volume(), within(0.01));

        // Check the calculated scaling factor (similar checks as the unit test)
        BigDecimal scalingFactor = new BigDecimal(parts[6]);
        assertThat(scalingFactor).isNotNull();
        assertThat(scalingFactor.compareTo(BigDecimal.ZERO)).isPositive();
        // Optionally, assert against a known expected value if calculated precisely
        BigDecimal expectedFactor = new BigDecimal("0.54317079"); // Example: Use actual calculated value if stable
        assertThat(scalingFactor).isCloseTo(expectedFactor, within(BigDecimal.valueOf(1e-8)));
    }
}