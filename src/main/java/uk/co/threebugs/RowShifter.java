package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public class RowShifter {
    /**
     * Converts floating point values in specified columns to integers by shifting decimal places.
     *
     * @param row           The CSV row as a string
     * @param columnIndices Indices of columns to process
     * @param decimalPlaces Number of decimal places to preserve in the integer representation
     * @return Modified CSV row with specified columns converted to integers
     */
    public static String shiftRow(String row, int[] columnIndices, int decimalPlaces) {
        String[] columns = row.split(",");
        BigDecimal multiplier = BigDecimal.TEN.pow(decimalPlaces);

        for (int columnIndex : columnIndices) {
            if (columnIndex < 0 || columnIndex >= columns.length) {
                continue;
            }

            try {
                String valueStr = columns[columnIndex].trim();
                BigDecimal value = new BigDecimal(valueStr);

                // Scale to the exact number of decimal places we want to preserve
                value = value.setScale(decimalPlaces, RoundingMode.HALF_UP);

                // Multiply by 10^decimalPlaces to get an integer
                BigDecimal shiftedValue = value.multiply(multiplier);

                // Convert to integer string (no decimal point)
                columns[columnIndex] = shiftedValue.stripTrailingZeros().toPlainString();
            } catch (NumberFormatException e) {
                log.warn("Column {} contains non-numeric value in row: {}", columnIndex, row);
                throw new RuntimeException("Invalid or non-numeric value in column " + columnIndex + " of row: " + row);
            }
        }

        return String.join(",", columns);
    }
}