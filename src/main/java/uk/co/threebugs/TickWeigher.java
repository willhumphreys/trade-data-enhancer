package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Scanner;

@Slf4j
class TickWeigher {

    private BigDecimal startingPrice = BigDecimal.valueOf(-1); // For close-based weighting
    private BigDecimal startingAtr = BigDecimal.valueOf(-1);   // For ATR-based weighting

    private BigDecimal smallestAtrWeighting = BigDecimal.valueOf(Double.MAX_VALUE); // Smallest ATR weighting encountered
    private BigDecimal largestAtrWeighting = BigDecimal.valueOf(Double.MIN_VALUE);  // Largest ATR weighting encountered

    /**
     * The main method to allow standalone execution of TickWeigher.
     */
    public static void main(String[] args) {
        TickWeigher tickWeigher = new TickWeigher();
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Welcome to TickWeigher!");
            while (true) {
                System.out.println("\nWhat would you like to do?");
                System.out.println("1: Set Starting Price");
                System.out.println("2: Calculate Weighting for Current Price");
                System.out.println("3: Reset Starting Price");
                System.out.println("0: Exit");
                System.out.print("Enter your choice: ");

                int choice = scanner.nextInt();
                switch (choice) {
                    case 1: // Set Starting Price
                        System.out.print("Enter Starting Price: ");
                        BigDecimal startingPrice = scanner.nextBigDecimal();
                        tickWeigher.startingPrice = startingPrice;
                        System.out.println("Starting Price has been set to: " + startingPrice);
                        break;

                    case 2: // Calculate Weighting
                        if (tickWeigher.startingPrice.compareTo(BigDecimal.ZERO) <= 0) {
                            System.out.println("Please set a valid Starting Price first.");
                            break;
                        }
                        System.out.print("Enter Current Price: ");
                        BigDecimal currentPrice = scanner.nextBigDecimal();
                        String weighting = tickWeigher.getWeighting(currentPrice);
                        System.out.println("Weighting (Current/Starting): " + weighting);
                        break;

                    case 3: // Reset Starting Price
                        tickWeigher.reset();
                        System.out.println("Starting Price has been reset.");
                        break;

                    case 0: // Exit
                        System.out.println("Exiting TickWeigher.");
                        return;

                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            }
        }
    }

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

        // Calculate ATR weighting
        BigDecimal weightingAtr = atr.divide(this.startingAtr, 2, RoundingMode.HALF_UP);
        BigDecimal formattedWeightingAtr = weightingAtr.setScale(2, RoundingMode.HALF_UP); // Ensure consistent formatting

        // Update smallest and largest ATR weightings
        if (formattedWeightingAtr.compareTo(smallestAtrWeighting) < 0) {
            smallestAtrWeighting = formattedWeightingAtr;
        }

        if (formattedWeightingAtr.compareTo(largestAtrWeighting) > 0) {
            largestAtrWeighting = formattedWeightingAtr;
        }

        return formattedWeightingAtr.toPlainString();
    }

    /**
     * Retrieves the smallest ATR weighting recorded so far.
     *
     * @return The smallest ATR weighting as a BigDecimal.
     */
    BigDecimal getSmallestAtrWeighting() {
        return smallestAtrWeighting.equals(BigDecimal.valueOf(Double.MAX_VALUE)) ? BigDecimal.ZERO : smallestAtrWeighting;
    }

    /**
     * Retrieves the largest ATR weighting recorded so far.
     *
     * @return The largest ATR weighting as a BigDecimal.
     */
    BigDecimal getLargestAtrWeighting() {
        return largestAtrWeighting.equals(BigDecimal.valueOf(Double.MIN_VALUE)) ? BigDecimal.ZERO : largestAtrWeighting;
    }

    /**
     * Resets the starting prices, ATRs, and min/max weightings, making the instance reusable.
     */
    void reset() {
        this.startingPrice = BigDecimal.valueOf(-1); // Reset the starting price
        this.startingAtr = BigDecimal.valueOf(-1);   // Reset the starting ATR
        this.smallestAtrWeighting = BigDecimal.valueOf(Double.MAX_VALUE);
        this.largestAtrWeighting = BigDecimal.valueOf(Double.MIN_VALUE);
    }

}