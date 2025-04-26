// In src/test/java/uk/co/threebugs/ATRScalingFactorSpliteratorTest.java
package uk.co.threebugs;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ATRScalingFactorSpliteratorTest {

    private static final int SHORT_PERIOD = 2;
    private static final int LONG_PERIOD = 4;
    private static final BigDecimal ALPHA = new BigDecimal("0.5");
    private static final int CALCULATION_SCALE = 8;
    private static final BigDecimal PRECISION = new BigDecimal("1E-" + CALCULATION_SCALE);

    // Helper to create sample data
    private ShiftedMinuteData createData(long timestamp, long open, long high, long low, long close, double volume) {
        return new ShiftedMinuteData(timestamp, open, high, low, close, volume);
    }

    // Helper to consume spliterator results into a list
    private List<ShiftedMinuteDataWithScalingFactor> consumeSpliterator(Spliterator<ShiftedMinuteDataWithScalingFactor> spliterator) {
        List<ShiftedMinuteDataWithScalingFactor> results = new ArrayList<>();
        while (spliterator.tryAdvance(results::add)) {
            // Consume all elements
        }
        return results;
    }

    @Test
    void tryAdvance_onEmptyIterator_returnsFalseAndProducesNoElements() {
        // Arrange
        List<ShiftedMinuteData> emptyData = Collections.emptyList();
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                emptyData.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );
        List<ShiftedMinuteDataWithScalingFactor> results = new ArrayList<>();

        // Act & Assert
        assertThat(spliterator.tryAdvance(results::add)).isFalse();
        assertThat(results).isEmpty();
    }

    @Test
    void tryAdvance_onSingleElementIterator_producesOneElementWithDefaultFactor() {
        // Arrange
        List<ShiftedMinuteData> singleData = List.of(
                createData(1L, 100, 110, 90, 105, 1000.0)
        );
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                singleData.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );
        List<ShiftedMinuteDataWithScalingFactor> results = new ArrayList<>();

        // Act & Assert
        assertThat(spliterator.tryAdvance(results::add)).isTrue(); // First element
        assertThat(spliterator.tryAdvance(results::add)).isFalse(); // No more elements

        assertThat(results).hasSize(1);
        assertThat(results.get(0).minuteData()).isEqualTo(singleData.get(0));
        assertThat(results.get(0).scalingFactor()).isEqualTo(BigDecimal.ONE); // Expect default factor 1
    }

    @Test
    void tryAdvance_iteratorShorterThanLongPeriod_producesDefaultFactors() {
        // Arrange
        List<ShiftedMinuteData> shortData = List.of(
                createData(1L, 100, 110, 90, 105, 1000.0),
                createData(2L, 105, 115, 100, 112, 1100.0),
                createData(3L, 112, 120, 110, 118, 1200.0) // Only 3 elements, LONG_PERIOD is 4
        );
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                shortData.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );

        // Act
        List<ShiftedMinuteDataWithScalingFactor> results = consumeSpliterator(spliterator);

        // Assert
        assertThat(results).hasSize(shortData.size());
        // Expect default factor 1 for all elements as long period is not met
        assertThat(results).extracting(ShiftedMinuteDataWithScalingFactor::scalingFactor)
                .containsOnly(BigDecimal.ONE);
        assertThat(results).extracting(ShiftedMinuteDataWithScalingFactor::minuteData)
                .containsExactlyElementsOf(shortData);
    }

    @Test
    void tryAdvance_iteratorMatchingLongPeriod_calculatesFactorForLastElement() {
        // Arrange
        List<ShiftedMinuteData> data = List.of(
                createData(1L, 100, 110, 90, 105, 1000.0), // TR=20
                createData(2L, 105, 115, 100, 112, 1100.0), // TR=15
                createData(3L, 112, 120, 110, 118, 1200.0), // TR=10
                createData(4L, 118, 125, 115, 122, 1300.0)  // TR=10. shortATR=10, longATR=13.75, factor=0.86363637
        );
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                data.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );
        BigDecimal expectedFactor4 = new BigDecimal("0.86363637");

        // Act
        List<ShiftedMinuteDataWithScalingFactor> results = consumeSpliterator(spliterator);

        // Assert
        assertThat(results).hasSize(data.size());
        // Expect default factor 1 until long period is met
        assertThat(results.get(0).scalingFactor()).isEqualTo(BigDecimal.ONE);
        assertThat(results.get(1).scalingFactor()).isEqualTo(BigDecimal.ONE);
        assertThat(results.get(2).scalingFactor()).isEqualTo(BigDecimal.ONE);
        // Expect calculated factor for the last element
        assertThat(results.get(3).scalingFactor()).isCloseTo(expectedFactor4, within(PRECISION));
    }

    @Test
    void tryAdvance_iteratorLongerThanPeriods_calculatesFactorsCorrectly() {
        // Arrange (Data from ATRScalingFactorAppenderTest)
        List<ShiftedMinuteData> inputData = List.of(
                createData(1L, 100, 110, 90, 105, 1000.0), // TR=20
                createData(2L, 105, 115, 100, 112, 1100.0), // TR=15
                createData(3L, 112, 120, 110, 118, 1200.0), // TR=10
                createData(4L, 118, 125, 115, 122, 1300.0), // TR=10. factor=0.86363637
                createData(5L, 122, 130, 120, 128, 1400.0)  // TR=10. factor=0.94444445
        );
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                inputData.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );
        BigDecimal expectedFactor4 = new BigDecimal("0.86363637");
        BigDecimal expectedFactor5 = new BigDecimal("0.94444445");

        // Act
        List<ShiftedMinuteDataWithScalingFactor> results = consumeSpliterator(spliterator);

        // Assert
        assertThat(results).hasSize(inputData.size());
        // Expect default factor 1 until long period is met
        assertThat(results.get(0).scalingFactor()).isEqualTo(BigDecimal.ONE);
        assertThat(results.get(1).scalingFactor()).isEqualTo(BigDecimal.ONE);
        assertThat(results.get(2).scalingFactor()).isEqualTo(BigDecimal.ONE);
        // Expect calculated factors
        assertThat(results.get(3).scalingFactor()).isCloseTo(expectedFactor4, within(PRECISION));
        assertThat(results.get(4).scalingFactor()).isCloseTo(expectedFactor5, within(PRECISION));
        assertThat(results).extracting(ShiftedMinuteDataWithScalingFactor::minuteData)
                .containsExactlyElementsOf(inputData);
    }

    @Test
    void tryAdvance_withZeroLongAtr_producesDefaultFactor() {
        // Arrange (Data from ATRScalingFactorAppenderTest)
        int shortP = 1;
        int longP = 2;
        List<ShiftedMinuteData> inputData = List.of(
                createData(1L, 100, 110, 90, 105, 1000.0), // TR = 20
                createData(2L, 105, 105, 105, 105, 1100.0), // TR = 0. shortATR=0, longATR=10, factor=0.5
                createData(3L, 105, 105, 105, 105, 1200.0)  // TR = 0. shortATR=0, longATR=0, factor=1 (default)
        );
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                inputData.iterator(), shortP, longP, ALPHA, CALCULATION_SCALE
        );
        BigDecimal expectedFactor2 = new BigDecimal("0.5");

        // Act
        List<ShiftedMinuteDataWithScalingFactor> results = consumeSpliterator(spliterator);

        // Assert
        assertThat(results).hasSize(inputData.size());
        assertThat(results.get(0).scalingFactor()).isEqualTo(BigDecimal.ONE); // Not enough data for long ATR -> default 1
        assertThat(results.get(1).scalingFactor()).isCloseTo(expectedFactor2, within(PRECISION)); // Calculated
        assertThat(results.get(2).scalingFactor()).isEqualTo(BigDecimal.ONE); // Long ATR becomes zero -> default 1
    }

    @Test
    void trySplit_returnsNull() {
        // Arrange
        List<ShiftedMinuteData> data = List.of(
                createData(1L, 100, 110, 90, 105, 1000.0),
                createData(2L, 105, 115, 100, 112, 1100.0)
        );
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                data.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );

        // Act & Assert
        assertThat(spliterator.trySplit()).isNull();
    }

    @Test
    void characteristics_areCorrect() {
        // Arrange
        List<ShiftedMinuteData> data = Collections.emptyList();
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                data.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );

        // Act & Assert
        assertThat(spliterator.characteristics())
                .isEqualTo(Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    }

    @Test
    void estimateSize_returnsMaxValue() {
        // Arrange
        List<ShiftedMinuteData> data = Collections.emptyList();
        ATRScalingFactorSpliterator spliterator = new ATRScalingFactorSpliterator(
                data.iterator(), SHORT_PERIOD, LONG_PERIOD, ALPHA, CALCULATION_SCALE
        );

        // Act & Assert
        assertThat(spliterator.estimateSize()).isEqualTo(Long.MAX_VALUE);
    }
}