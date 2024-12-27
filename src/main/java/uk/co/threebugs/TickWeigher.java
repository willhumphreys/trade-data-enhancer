package uk.co.threebugs;

import java.math.BigDecimal;
import java.math.RoundingMode;

class TickWeigher {

    private BigDecimal startingPrice = BigDecimal.valueOf(-1); // For close-based weighting
    private BigDecimal startingAtr = BigDecimal.valueOf(-1);   // For ATR-based weighting

    /**
     * Calculates the weighting relative to the starting price and returns it as a formatted String.
     *
     * @param closePrice The closing price for the tick.
     * @return The weighting as a formatted String with 2 decimal places.
     */
    String getWeighting(final BigDecimal closePrice) {
        if (closePrice.compareTo(BigDecimal.ZERO) < 0) {
            return "1.00"; // Default weighting for invalid prices
        }

        if (this.startingPrice.compareTo(BigDecimal.ZERO) < 0) {
            this.startingPrice = closePrice; // Initialize starting price
        }

        // Calculate weighting and ensure 2 decimal places
        BigDecimal weighting = closePrice.divide(this.startingPrice, 2, RoundingMode.HALF_UP);
        return weighting.setScale(2, RoundingMode.HALF_UP).toPlainString(); // Ensure consistent formatting
    }

    /**
     * Calculates the ATR-based weighting relative to the starting ATR.
     *
     * @param atr The ATR value for the tick.
     * @return The ATR weighting as a formatted String with 2 decimal places.
     */
    String getWeightingAtr(final BigDecimal atr) {
        if (atr.compareTo(BigDecimal.ZERO) <= 0) {
            return "1.00"; // Default weighting for invalid ATRs
        }

        // Initialize starting ATR if not already initialized
        if (this.startingAtr.compareTo(BigDecimal.ZERO) <= 0) {
            this.startingAtr = atr; // Set starting ATR to the current ATR value
        }

        // Avoid division by zero or uninitialized startingAtr
        if (this.startingAtr.compareTo(BigDecimal.ZERO) <= 0) {
            return "1.00"; // Default value in case startingAtr is invalid
        }

        // Calculate ATR weighting and ensure 2 decimal places
        BigDecimal weightingAtr = atr.divide(this.startingAtr, 2, RoundingMode.HALF_UP);
        return weightingAtr.setScale(2, RoundingMode.HALF_UP).toPlainString(); // Ensure consistent formatting
    }

    /**
     * Resets the starting prices and ATRs, making the instance reusable.
     */
    void reset() {
        this.startingPrice = BigDecimal.valueOf(-1); // Reset the starting price
        this.startingAtr = BigDecimal.valueOf(-1);   // Reset the starting ATR
    }
}