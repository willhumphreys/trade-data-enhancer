package uk.co.threebugs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

class HybridScalingAppenderTest {

    private HybridScalingAppender dailyAppender;
    private HybridScalingAppender hourlyAppender;
    private List<ShiftedData> sampleData;

    @BeforeEach
    void setUp() {
        // Using realistic ATR periods and alpha
        dailyAppender = new HybridScalingAppender(5, 14, 0.5, TimeFrame.DAILY);
        hourlyAppender = new HybridScalingAppender(5, 14, 0.5, TimeFrame.HOURLY);

        // Sample data for testing - needs enough entries for ATR calculation
        sampleData = Arrays.asList(new ShiftedData(1678886400, 10000, 10500, 9800, 10200, 10.5), // 2023-03-15 13:00:00
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
    void constructor_shouldThrowException_whenAlphaIsInvalid() {
        assertThatThrownBy(() -> new HybridScalingAppender(5, 14, -0.1, TimeFrame.DAILY)).isInstanceOf(IllegalArgumentException.class).hasMessage("Alpha must be between 0 and 1");

        assertThatThrownBy(() -> new HybridScalingAppender(5, 14, 1.1, TimeFrame.DAILY)).isInstanceOf(IllegalArgumentException.class).hasMessage("Alpha must be between 0 and 1");
    }

    @Test
    void appendScalingFactors_shouldCalculateFactors_forDailyTimeFrame() {
        Stream<ShiftedData> dataStream = sampleData.stream();
        List<HybridScalingAppender.ShiftedDataWithHybridScaling> result = dailyAppender.appendScalingFactors(dataStream).collect(Collectors.toList());

        assertThat(result).hasSize(sampleData.size());

        // Verify a later entry (index 13, requires 14 periods for long ATR)
        // Note: Exact values depend heavily on ta4j's ATR calculation specifics.
        // We check if it's a plausible positive value.
        BigDecimal factorAtIndex13 = result.get(13).hybridScalingFactor();
        assertThat(factorAtIndex13).isNotNull();
        // Check that the factor is positive, as ATR values should be positive with this data
        assertThat(factorAtIndex13.compareTo(BigDecimal.ZERO)).isPositive();
        // If a specific expected value is known after verifying ta4j calculation, assert it here:
        // BigDecimal expectedFactor = new BigDecimal("1.02853640"); // Example: Use actual calculated value if stable
        // assertThat(factorAtIndex13).isCloseTo(expectedFactor, within(BigDecimal.valueOf(1e-8)));


        // Check that original data is preserved
        assertThat(result.get(5).timestamp()).isEqualTo(sampleData.get(5).timestamp());
        assertThat(result.get(5).open()).isEqualTo(sampleData.get(5).open());
        assertThat(result.get(5).high()).isEqualTo(sampleData.get(5).high());
        assertThat(result.get(5).low()).isEqualTo(sampleData.get(5).low());
        assertThat(result.get(5).close()).isEqualTo(sampleData.get(5).close());
        assertThat(result.get(5).volume()).isEqualTo(sampleData.get(5).volume());
    }

    @Test
    void appendScalingFactors_shouldReturnOne_forHourlyTimeFrame() {
        Stream<ShiftedData> dataStream = sampleData.stream();
        List<HybridScalingAppender.ShiftedDataWithHybridScaling> result = hourlyAppender.appendScalingFactors(dataStream).collect(Collectors.toList());

        assertThat(result).hasSize(sampleData.size());
        result.forEach(entry -> assertThat(entry.hybridScalingFactor()).isEqualByComparingTo(BigDecimal.ONE));

        // Check that original data is preserved
        assertThat(result.get(5).timestamp()).isEqualTo(sampleData.get(5).timestamp());
        assertThat(result.get(5).close()).isEqualTo(sampleData.get(5).close());
        assertThat(result.get(5).volume()).isEqualTo(sampleData.get(5).volume());
    }

    @Test
    void writeStreamToFile_shouldWriteCorrectFormat(@TempDir Path tempDir) throws IOException {
        Path outputFile = tempDir.resolve("output.csv");
        List<HybridScalingAppender.ShiftedDataWithHybridScaling> testEntries = List.of(new HybridScalingAppender.ShiftedDataWithHybridScaling(1678886400, 100, 110, 95, 105, 10.5, new BigDecimal("1.23456789")), new HybridScalingAppender.ShiftedDataWithHybridScaling(1678886460, 105, 115, 100, 110, 12.0, new BigDecimal("0.98765432")));

        dailyAppender.writeStreamToFile(testEntries.stream(), outputFile);

        List<String> lines = Files.readAllLines(outputFile);

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("Timestamp,Open,High,Low,Close,Volume,HybridScalingFactor");
        assertThat(lines.get(1)).isEqualTo("1678886400,100,110,95,105,10.50,1.23456789");
        assertThat(lines.get(2)).isEqualTo("1678886460,105,115,100,110,12.00,0.98765432");
    }

    // Direct test for the calculation logic if needed, though covered by appendScalingFactors test
    @Test
    void calculateHybridScalingFactor_directTest() {
        // Use reflection or make the method package-private/public for direct testing if necessary
        // Or test via appendScalingFactors as done above.
        // Example direct calculation (assuming access or using known values from appendScalingFactors test)
        BigDecimal shortATR = new BigDecimal("150.50");
        BigDecimal longATR = new BigDecimal("200.75");
        BigDecimal price = new BigDecimal("11000");
        BigDecimal baselinePrice = new BigDecimal("10500");
        BigDecimal alpha = new BigDecimal("0.5");
        int scale = 8;

        BigDecimal normalizedATRRatio = shortATR.divide(price, scale, RoundingMode.HALF_UP).divide(longATR.divide(baselinePrice, scale, RoundingMode.HALF_UP), scale, RoundingMode.HALF_UP); // Approx 0.7165
        BigDecimal absoluteATRRatio = shortATR.divide(longATR, scale, RoundingMode.HALF_UP); // Approx 0.7497

        BigDecimal expected = alpha.multiply(normalizedATRRatio).add(BigDecimal.ONE.subtract(alpha).multiply(absoluteATRRatio)); // Approx 0.5 * 0.7165 + 0.5 * 0.7497 = 0.7331

        // This requires making calculateHybridScalingFactor accessible or replicating its logic
        BigDecimal actual = dailyAppender.calculateHybridScalingFactor(shortATR, longATR, price, baselinePrice);
        assertThat(actual).isCloseTo(expected, within(BigDecimal.valueOf(1e-8)));
    }

    @Test
    void calculateHybridScalingFactor_shouldReturnZero_whenAtrIsZero() {
        // This test requires making calculateHybridScalingFactor accessible
        // Or testing via appendScalingFactors with data that produces zero ATR (e.g., constant price)
        BigDecimal price = new BigDecimal("10000");
        BigDecimal baselinePrice = new BigDecimal("10000");

        //Assuming calculateHybridScalingFactor is made accessible for testing:
        BigDecimal result1 = dailyAppender.calculateHybridScalingFactor(BigDecimal.ZERO, BigDecimal.TEN, price, baselinePrice);
        BigDecimal result2 = dailyAppender.calculateHybridScalingFactor(BigDecimal.TEN, BigDecimal.ZERO, price, baselinePrice);
        BigDecimal result3 = dailyAppender.calculateHybridScalingFactor(BigDecimal.ZERO, BigDecimal.ZERO, price, baselinePrice);

        assertThat(result1).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(result2).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(result3).isEqualByComparingTo(BigDecimal.ZERO);

        // Alternative: Test via appendScalingFactors with flat data
        List<ShiftedData> flatData = Arrays.asList(new ShiftedData(1678886400, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886460, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886520, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886580, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886640, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886700, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886760, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886820, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886880, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678886940, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678887000, 10000, 10000, 10000, 10000, 10.0), // Continue with enough data for ATR calculation
                new ShiftedData(1678887060, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678887120, 10000, 10000, 10000, 10000, 10.0), new ShiftedData(1678887180, 10000, 10000, 10000, 10000, 10.0));

        // Need an appender instance for this test
        HybridScalingAppender flatDataAppender = new HybridScalingAppender(5, 14, 0.5, TimeFrame.DAILY);
        List<HybridScalingAppender.ShiftedDataWithHybridScaling> result = flatDataAppender.appendScalingFactors(flatData.stream()).toList();

        // ATR will be zero for flat data after the initial period
        // Check an index where both ATRs should have stabilized to zero
        assertThat(result.get(13).hybridScalingFactor()).isEqualByComparingTo(BigDecimal.ZERO);
    }

}

