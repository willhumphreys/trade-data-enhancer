package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.assertj.core.api.Assertions.*;

class ATRCalculatorTest {

    private static final int TEST_SCALE = 8;
    private static final BigDecimal TEST_PRECISION = new BigDecimal("1E-" + TEST_SCALE);

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -10})
    void constructor_withNonPositivePeriod_throwsIllegalArgumentException(int period) {
        assertThatThrownBy(() -> new ATRCalculator(period))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Period must be positive");
    }

    @Test
    void getATR_beforeWindowIsFull_returnsNull() {
        // Arrange
        int period = 3;
        ATRCalculator calculator = new ATRCalculator(period);

        // Act & Assert
        calculator.addBar(100, 90, 95); // Bar 1
        assertThat(calculator.getATR()).isNull();

        calculator.addBar(105, 95, 100); // Bar 2
        assertThat(calculator.getATR()).isNull();
    }

    @Test
    void getATR_whenWindowIsFull_returnsCorrectATR() {
        // Arrange
        int period = 3;
        ATRCalculator calculator = new ATRCalculator(period);

        // Bar 1: H=100, L=90, C=95. prevC=null. TR = H-L = 10
        calculator.addBar(bd(100), bd(90), bd(95));
        // Bar 2: H=105, L=95, C=100. prevC=95. TR = max(H-L, abs(H-prevC), abs(L-prevC)) = max(10, 10, 0) = 10
        calculator.addBar(bd(105), bd(95), bd(100));
        // Bar 3: H=110, L=98, C=108. prevC=100. TR = max(12, 10, 2) = 12
        calculator.addBar(bd(110), bd(98), bd(108));

        // Expected ATR = (10 + 10 + 12) / 3 = 32 / 3 = 10.66666667
        BigDecimal expectedATR = bd(32).divide(bd(period), TEST_SCALE, RoundingMode.HALF_UP);

        // Act
        BigDecimal actualATR = calculator.getATR();

        // Assert
        assertThat(actualATR).isNotNull();
        assertThat(actualATR).isCloseTo(expectedATR, within(TEST_PRECISION));
    }

    @Test
    void getATR_withSlidingWindow_returnsCorrectATR() {
        // Arrange
        int period = 3;
        ATRCalculator calculator = new ATRCalculator(period);

        // Bar 1: TR = 10
        calculator.addBar(bd(100), bd(90), bd(95));
        // Bar 2: TR = 10
        calculator.addBar(bd(105), bd(95), bd(100));
        // Bar 3: TR = 12. Window=[10, 10, 12]. Sum=32. ATR = 32/3 = 10.66666667
        calculator.addBar(bd(110), bd(98), bd(108));
        // Bar 4: H=115, L=105, C=112. prevC=108. TR = max(10, 7, 3) = 10. Window=[10, 12, 10]. Sum=32. ATR = 32/3 = 10.66666667
        calculator.addBar(bd(115), bd(105), bd(112));

        // Expected ATR = (10 + 12 + 10) / 3 = 32 / 3 = 10.66666667
        BigDecimal expectedATR = bd(32).divide(bd(period), TEST_SCALE, RoundingMode.HALF_UP);

        // Act
        BigDecimal actualATR = calculator.getATR();

        // Assert
        assertThat(actualATR).isNotNull();
        assertThat(actualATR).isCloseTo(expectedATR, within(TEST_PRECISION));

        // Bar 5: H=120, L=110, C=115. prevC=112. TR = max(10, 8, 2) = 10. Window=[12, 10, 10]. Sum=32. ATR = 32/3 = 10.66666667
        calculator.addBar(bd(120), bd(110), bd(115));
        // Expected ATR = (12 + 10 + 10) / 3 = 32 / 3 = 10.66666667
        expectedATR = bd(32).divide(bd(period), TEST_SCALE, RoundingMode.HALF_UP);

        // Act
        actualATR = calculator.getATR();

        // Assert
        assertThat(actualATR).isNotNull();
        assertThat(actualATR).isCloseTo(expectedATR, within(TEST_PRECISION));
    }

    @Test
    void addBar_withLongValues_calculatesCorrectly() {
        // Arrange
        int period = 2;
        ATRCalculator calculator = new ATRCalculator(period);

        // Bar 1: H=100, L=90, C=95. TR = 10
        calculator.addBar(100L, 90L, 95L);
        // Bar 2: H=105, L=95, C=100. TR = 10
        calculator.addBar(105L, 95L, 100L);

        // Expected ATR = (10 + 10) / 2 = 10
        BigDecimal expectedATR = bd(10);

        // Act
        BigDecimal actualATR = calculator.getATR();

        // Assert
        assertThat(actualATR).isNotNull();
        assertThat(actualATR).isCloseTo(expectedATR, within(TEST_PRECISION));
    }

    // Helper to create BigDecimal
    private BigDecimal bd(double value) {
        return BigDecimal.valueOf(value);
    }
}