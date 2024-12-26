package uk.co.threebugs;

import java.math.BigDecimal;
import java.math.RoundingMode;

class TickWeigher {

    private BigDecimal startingPrice = BigDecimal.valueOf(-1); // More concise initialization

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
     * Resets the starting price, making the instance reusable.
     */
    void reset() {
        this.startingPrice = BigDecimal.valueOf(-1); // Reset to the same initial value
    }
}