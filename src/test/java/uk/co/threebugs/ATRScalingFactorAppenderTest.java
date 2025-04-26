package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

class ATRScalingFactorAppenderTest {

    private static final int CALCULATION_SCALE = 8;

    @Test
    void testAppendScalingFactor() {
        // Arrange
        ATRScalingFactorAppender appender = new ATRScalingFactorAppender();
        int shortPeriod = 2;
        int longPeriod = 4;
        BigDecimal alpha = new BigDecimal("0.5");

        // Sample data - enough to fill the long period window
        List<ShiftedMinuteData> inputData = List.of(
                new ShiftedMinuteData(1L, 100, 110, 90, 105, 1000.0), // TR = 20 (110-90)
                new ShiftedMinuteData(2L, 105, 115, 100, 112, 1100.0), // TR = max(15, 10, 5) = 15
                new ShiftedMinuteData(3L, 112, 120, 110, 118, 1200.0), // TR = max(10, 8, 2) = 10
                new ShiftedMinuteData(4L, 118, 125, 115, 122, 1300.0), // TR = max(10, 7, 3) = 10
                new ShiftedMinuteData(5L, 122, 130, 120, 128, 1400.0)  // TR = max(10, 8, 2) = 10
        );

        // Expected calculations:
        // Bar 1: TR=20. shortATR=null, longATR=null, factor=null
        // Bar 2: TR=15. shortATR=(20+15)/2=17.5, longATR=null, factor=null
        // Bar 3: TR=10. shortATR=(15+10)/2=12.5, longATR=null, factor=null
        // Bar 4: TR=10. shortATR=(10+10)/2=10.0, longATR=(20+15+10+10)/4=13.75, ratio=10/13.75=0.72727273, factor=0.5*0.72727273 + 0.5 = 0.86363637
        // Bar 5: TR=10. shortATR=(10+10)/2=10.0, longATR=(15+10+10+10)/4=11.25, ratio=10/11.25=0.88888889, factor=0.5*0.88888889 + 0.5 = 0.94444444

        BigDecimal expectedFactor4 = new BigDecimal("0.86363637"); // Calculated manually as above
        BigDecimal expectedFactor5 = new BigDecimal("0.94444444"); // Calculated manually as above

        // Act
        Stream<ShiftedMinuteDataWithScalingFactor> resultStream = appender.appendScalingFactor(inputData.stream(), shortPeriod, longPeriod, alpha);
        List<ShiftedMinuteDataWithScalingFactor> results = resultStream.collect(Collectors.toList());

        // Assert
        assertThat(results).hasSize(inputData.size());

        // Check original data is preserved
        for (int i = 0; i < inputData.size(); i++) {
            assertThat(results.get(i).minuteData()).isEqualTo(inputData.get(i));
        }

        // Check scaling factors (null until longPeriod is met)
        assertThat(results.get(0).scalingFactor()).isNull();
        assertThat(results.get(1).scalingFactor()).isNull();
        assertThat(results.get(2).scalingFactor()).isNull();

        // Check calculated factors (use offset for BigDecimal comparison)
        assertThat(results.get(3).scalingFactor())
                .isNotNull()
                .isCloseTo(expectedFactor4, within(new BigDecimal("1E-" + CALCULATION_SCALE)));
        assertThat(results.get(4).scalingFactor())
                .isNotNull()
                .isCloseTo(expectedFactor5, within(new BigDecimal("1E-" + CALCULATION_SCALE)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-0.1", "1.1"})
    void testAppendScalingFactor_invalidAlpha_throwsException(String alphaString) {
        // Arrange
        ATRScalingFactorAppender appender = new ATRScalingFactorAppender();
        BigDecimal invalidAlpha = new BigDecimal(alphaString);
        List<ShiftedMinuteData> inputData = List.of(
                new ShiftedMinuteData(1L, 100, 110, 90, 105, 1000.0)
        );

        // Act & Assert
        assertThatThrownBy(() -> appender.appendScalingFactor(inputData.stream(), 14, 30, invalidAlpha))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Alpha must be between 0 and 1, inclusive.");
    }

    @Test
    void testAppendScalingFactor_zeroLongAtr() {
        // Arrange
        ATRScalingFactorAppender appender = new ATRScalingFactorAppender();
        int shortPeriod = 1;
        int longPeriod = 2; // Need long period >= 2 for ATR calc
        BigDecimal alpha = new BigDecimal("0.5");

        // Data designed to produce zero TR after the first bar
        List<ShiftedMinuteData> inputData = List.of(
                new ShiftedMinuteData(1L, 100, 110, 90, 105, 1000.0), // TR = 20
                new ShiftedMinuteData(2L, 105, 105, 105, 105, 1100.0), // TR = max(0, 0, 0) = 0
                new ShiftedMinuteData(3L, 105, 105, 105, 105, 1200.0)  // TR = max(0, 0, 0) = 0
        );

        // Expected calculations:
        // Bar 1: TR=20. shortATR=20/1=20, longATR=null, factor=null
        // Bar 2: TR=0. shortATR=0/1=0, longATR=(20+0)/2=10, ratio=0/10=0, factor=0.5*0+0.5=0.5
        // Bar 3: TR=0. shortATR=0/1=0, longATR=(0+0)/2=0. Division by zero! factor=null

        BigDecimal expectedFactor2 = new BigDecimal("0.5");

        // Act
        Stream<ShiftedMinuteDataWithScalingFactor> resultStream = appender.appendScalingFactor(inputData.stream(), shortPeriod, longPeriod, alpha);
        List<ShiftedMinuteDataWithScalingFactor> results = resultStream.collect(Collectors.toList());

        // Assert
        assertThat(results).hasSize(inputData.size());
        assertThat(results.get(0).scalingFactor()).isNull(); // Not enough data for long ATR
        assertThat(results.get(1).scalingFactor())
                .isNotNull()
                .isCloseTo(expectedFactor2, within(new BigDecimal("1E-" + CALCULATION_SCALE)));
        assertThat(results.get(2).scalingFactor()).isNull(); // Long ATR becomes zero
    }
}