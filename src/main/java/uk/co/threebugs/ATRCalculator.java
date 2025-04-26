package uk.co.threebugs;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedList;

/**
 * Utility to compute Average True Range (ATR) using BigDecimal for precision.
 */
public class ATRCalculator {
    private static final int SCALE = 8;
    private final int period;
    private final LinkedList<BigDecimal> window = new LinkedList<>();
    private BigDecimal sumTR = BigDecimal.ZERO;
    private BigDecimal prevClose = null;

    public ATRCalculator(int period) {
        if (period <= 0) throw new IllegalArgumentException("Period must be positive");
        this.period = period;
    }

    /**
     * Update ATR with a new bar of data.
     */
    public void addBar(BigDecimal high, BigDecimal low, BigDecimal close) {
        BigDecimal trueRange;
        if (prevClose == null) {
            trueRange = high.subtract(low);
        } else {
            BigDecimal term1 = high.subtract(low);
            BigDecimal term2 = high.subtract(prevClose).abs();
            BigDecimal term3 = low.subtract(prevClose).abs();
            trueRange = term1.max(term2).max(term3);
        }
        window.addLast(trueRange);
        sumTR = sumTR.add(trueRange);
        if (window.size() > period) {
            sumTR = sumTR.subtract(window.removeFirst());
        }
        prevClose = close;
    }

    public void addBar(long high, long low, long close) {
        addBar(BigDecimal.valueOf(high), BigDecimal.valueOf(low), BigDecimal.valueOf(close));
    }

    public BigDecimal getATR() {
        if (window.size() < period) return null;
        return sumTR.divide(BigDecimal.valueOf(period), SCALE, RoundingMode.HALF_UP);
    }
}