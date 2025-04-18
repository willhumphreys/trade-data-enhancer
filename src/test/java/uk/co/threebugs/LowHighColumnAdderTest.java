package uk.co.threebugs;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class LowHighColumnAdderTest {

    private final LowHighColumnAdder columnAdder = new LowHighColumnAdder();

    // Constants for the indices in the test data
    private static final int TEST_HIGH_COL_INDEX = 1;
    private static final int TEST_LOW_COL_INDEX = 0;
    private static final int TEST_FIXED_LOW_COL_INDEX = 2; // After adding columns
    private static final int TEST_FIXED_HIGH_COL_INDEX = 3; // After adding columns

    @Test
    void testProcessRow_shouldHandleFirstRow() {
        String row = "100,200";

        String result = columnAdder.processRow(null, row, TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);

        assertThat(result).isEqualTo("100,200,100,200");
    }

    @Test
    void testProcessRow_shouldThrowExceptionForMalformedRow() {
        String row = "not,a,valid,row";

        assertThatThrownBy(() ->
                columnAdder.processRow(null, row, 5, 6, 7, 8)
        ).isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    void testProcessRow_shouldThrowExceptionForInvalidNumberFormat() {
        String row = "abc,def";

        assertThatThrownBy(() ->
                columnAdder.processRow(null, row, TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                        TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX)
        ).isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testProcessRow_shouldHandleEmptyPreviousRow() {
        String row = "100,200";

        String result = columnAdder.processRow(null, row, TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);

        assertThat(result).isEqualTo("100,200,100,200");
    }

    // Keep the existing tests for addFixedLowAndHighColumns unchanged
    // ...



    @Test
    void testProcessRow_shouldHandleSubsequentRows() {
        String previousRow = "100,200,100,200";
        String currentRow = "80,180";

        String result = columnAdder.processRow(previousRow, currentRow, TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);

        assertThat(result).isEqualTo("80,180,100,180");
    }

    @Test
    void testProcessRow_shouldUseProcessedPreviousRow() {
        // Prepare a previous row that has already been processed and has fixedLow and fixedHigh columns
        String previousRow = "100,200,100,200";  // low,high,fixedLow,fixedHigh
        String currentRow = "150,250";  // low,high

        String result = columnAdder.processRow(previousRow, currentRow,
                TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);

        // Expected result: 150,250,150,200
        // Current range overlaps with previous fixed range, so fixedLow = max(100,150) = 150, fixedHigh = min(200,250) = 200
        assertThat(result).isEqualTo("150,250,150,200");
    }

    @Test
    void testProcessRow_shouldHandleNonOverlappingRanges() {
        // Test case 1: Current range is below previous range
        String previousRow = "300,400,300,400";  // low,high,fixedLow,fixedHigh
        String currentRow = "100,200";  // low,high - completely below previous

        String result1 = columnAdder.processRow(previousRow, currentRow,
                TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);

        // Expected: 100,200,100,300
        // Current completely below, so fixedLow = currentLow and fixedHigh = previousFixedLow
        assertThat(result1).isEqualTo("100,200,100,300");

        // Test case 2: Current range is above previous range
        previousRow = "100,200,100,200";  // low,high,fixedLow,fixedHigh
        currentRow = "300,400";  // low,high - completely above previous

        String result2 = columnAdder.processRow(previousRow, currentRow,
                TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);

        // Expected: 300,400,200,400
        // Current completely above, so fixedLow = previousFixedHigh and fixedHigh = currentHigh
        assertThat(result2).isEqualTo("300,400,200,400");
    }

    @Test
    void testProcessRow_shouldMaintainContinuousRangeAcrossAllProcessedRows() {
        // First row
        String firstRow = "100,200";
        String processedFirstRow = columnAdder.processRow(null, firstRow,
                TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);
        // Expected: 100,200,100,200
        assertThat(processedFirstRow).isEqualTo("100,200,100,200");

        // Second row (above first row)
        String secondRow = "300,400";
        String processedSecondRow = columnAdder.processRow(processedFirstRow, secondRow,
                TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);
        // Expected: 300,400,200,400
        assertThat(processedSecondRow).isEqualTo("300,400,200,400");

        // Third row (in between first and second)
        String thirdRow = "210,290";
        String processedThirdRow = columnAdder.processRow(processedSecondRow, thirdRow,
                TEST_HIGH_COL_INDEX, TEST_LOW_COL_INDEX,
                TEST_FIXED_LOW_COL_INDEX, TEST_FIXED_HIGH_COL_INDEX);
        // Expected: 210,290,200,290
        assertThat(processedThirdRow).isEqualTo("210,290,200,290");

        // Verify that there's no gap in the fixed ranges
        String[] firstProcessed = processedFirstRow.split(",");
        String[] secondProcessed = processedSecondRow.split(",");
        String[] thirdProcessed = processedThirdRow.split(",");

        long firstFixedHigh = Long.parseLong(firstProcessed[TEST_FIXED_HIGH_COL_INDEX]);
        long secondFixedLow = Long.parseLong(secondProcessed[TEST_FIXED_LOW_COL_INDEX]);
        long thirdFixedLow = Long.parseLong(thirdProcessed[TEST_FIXED_LOW_COL_INDEX]);

        // Verify that second row's fixed low matches first row's fixed high (no gap)
        assertThat(secondFixedLow).isEqualTo(firstFixedHigh);

        // Verify that third row's fixed low matches second row's fixed low (maintains continuity)
        assertThat(thirdFixedLow).isEqualTo(secondFixedLow);
    }
}