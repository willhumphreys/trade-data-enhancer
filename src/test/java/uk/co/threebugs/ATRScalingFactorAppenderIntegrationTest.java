// In src/test/java/uk/co/threebugs/ATRScalingFactorAppenderIntegrationTest.java
package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ATRScalingFactorAppenderIntegrationTest {

    @TempDir
    Path tempDir; // JUnit 5 temporary directory

    @Test
    void testAppendAndWriteScalingFactor() throws IOException {
        // Arrange
        ATRScalingFactorAppender appender = new ATRScalingFactorAppender();
        int shortPeriod = 2;
        int longPeriod = 4;
        BigDecimal alpha = new BigDecimal("0.5");
        Path outputFile = tempDir.resolve("output_with_factors.csv");

        // Sample input data (same as unit test for consistency)
        List<ShiftedMinuteData> inputData = List.of(
                new ShiftedMinuteData(1L, 100, 110, 90, 105, 1000.0),
                new ShiftedMinuteData(2L, 105, 115, 100, 112, 1100.0),
                new ShiftedMinuteData(3L, 112, 120, 110, 118, 1200.0),
                new ShiftedMinuteData(4L, 118, 125, 115, 122, 1300.0),
                new ShiftedMinuteData(5L, 122, 130, 120, 128, 1400.0)
        );

        // Expected output content (including header and formatted data)
        // Factors: 1 (default), 1 (default), 1 (default), 0.86363637, 0.94444445
        // Default factor 1 is formatted to 8 decimal places by formatDataEntry
        String defaultFactorString = "1.00000000";
        List<String> expectedOutputLines = List.of(
                "Timestamp,open,high,low,close,volume,scalingFactor",
                "1,100,110,90,105,1000.00," + defaultFactorString, // Default factor 1
                "2,105,115,100,112,1100.00," + defaultFactorString, // Default factor 1
                "3,112,120,110,118,1200.00," + defaultFactorString, // Default factor 1
                "4,118,125,115,122,1300.00,0.86363637",
                "5,122,130,120,128,1400.00,0.94444445"
        );

        // Act
        // 1. Calculate factors
        Stream<ShiftedMinuteDataWithScalingFactor> resultStream = appender.appendScalingFactor(
                inputData.stream(), shortPeriod, longPeriod, alpha
        );

        // 2. Write to file
        appender.writeStreamToFile(resultStream, outputFile);

        // Assert
        // 3. Read the file back
        assertThat(outputFile).exists();
        List<String> actualOutputLines = Files.readAllLines(outputFile);

        // 4. Verify content
        assertThat(actualOutputLines).containsExactlyElementsOf(expectedOutputLines);
    }
}