package uk.co.threebugs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowShifterTest {

    static Stream<Arguments> providePerformanceTestCases() {
        // Create a large row with many columns for performance testing
        StringBuilder largeRow = new StringBuilder();
        StringBuilder expectedResult = new StringBuilder();

        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                largeRow.append(",");
                expectedResult.append(",");
            }
            largeRow.append(i).append(".").append(i % 10);
            expectedResult.append(i * 10 + i % 10);
        }

        int[] allIndices = new int[100];
        for (int i = 0; i < 100; i++) {
            allIndices[i] = i;
        }

        return Stream.of(
                Arguments.of(largeRow.toString(), allIndices, 1, expectedResult.toString())
        );
    }

    @Test
    @DisplayName("Should convert floating point values to integers with consistent decimal places")
    void shouldConvertFloatsToIntegers() {
        // Given
        String row = "123,1.23,45.678,7.1,9.9999";
        int[] columnIndices = {1, 2, 3, 4};
        int decimalPlaces = 2;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("123,123,4568,710,1000");
    }

    @Test
    @DisplayName("Should round values according to specified decimal places")
    void shouldRoundValuesCorrectly() {
        // Given
        String row = "1.25,1.35,1.45,1.55,1.65";
        int[] columnIndices = {0, 1, 2, 3, 4};
        int decimalPlaces = 1;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then - testing HALF_UP rounding mode
        assertThat(result).isEqualTo("13,14,15,16,17");
    }

    @Test
    @DisplayName("Should handle integers properly")
    void shouldHandleIntegers() {
        // Given
        String row = "123,456,789";
        int[] columnIndices = {0, 1, 2};
        int decimalPlaces = 2;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("12300,45600,78900");
    }

    @Test
    @DisplayName("Should handle a mix of different precision values")
    void shouldHandleMixOfDifferentPrecisions() {
        // Given
        String row = "value1,1.5,1.55,1.555,1.5555";
        int[] columnIndices = {1, 2, 3, 4};
        int decimalPlaces = 3;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("value1,1500,1550,1555,1556");
    }

    @Test
    @DisplayName("Should handle very small values")
    void shouldHandleVerySmallValues() {
        // Given
        String row = "0.0001,0.00001,0.000001";
        int[] columnIndices = {0, 1, 2};
        int decimalPlaces = 4;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("1,0,0");
    }

    @Test
    @DisplayName("Should handle negative numbers")
    void shouldHandleNegativeNumbers() {
        // Given
        String row = "-1.23,-4.56,-7.89";
        int[] columnIndices = {0, 1, 2};
        int decimalPlaces = 2;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("-123,-456,-789");
    }

    @Test
    @DisplayName("Should handle scientific notation")
    void shouldHandleScientificNotation() {
        // Given
        String row = "1.23E2,4.56E-2,7.89E1";
        int[] columnIndices = {0, 1, 2};
        int decimalPlaces = 2;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("12300,5,7890");
    }

    @Test
    @DisplayName("Should skip invalid column indices")
    void shouldSkipInvalidColumnIndices() {
        // Given
        String row = "1.1,2.2,3.3";
        int[] columnIndices = {-1, 5, 1}; // -1 and 5 should be skipped, only index 1 processed
        int decimalPlaces = 1;

        // When
        String result = RowShifter.shiftRow(row, columnIndices, decimalPlaces);

        // Then
        assertThat(result).isEqualTo("1.1,22,3.3");
    }

    @Test
    @DisplayName("Should throw exception for non-numeric values")
    void shouldThrowExceptionForNonNumericValues() {
        // Given
        String row = "abc,1.23,xyz";
        int[] columnIndices = {0, 1, 2};
        int decimalPlaces = 2;

        // Then
        assertThatThrownBy(() -> RowShifter.shiftRow(row, columnIndices, decimalPlaces))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Invalid or non-numeric value");
    }

    @ParameterizedTest
    @MethodSource("providePerformanceTestCases")
    @DisplayName("Should efficiently process rows (performance test)")
    void shouldProcessRowsEfficiently(String row, int[] indices, int places, String expected) {
        // When
        String result = RowShifter.shiftRow(row, indices, places);

        // Then
        assertThat(result).isEqualTo(expected);
    }
}