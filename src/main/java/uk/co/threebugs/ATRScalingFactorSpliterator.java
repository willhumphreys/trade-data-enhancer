// Create a new file src/main/java/uk/co/threebugs/ATRScalingFactorSpliterator.java
package uk.co.threebugs;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * A Spliterator that calculates ATR scaling factors sequentially from a source iterator
 * without collecting intermediate results into memory.
 */
public class ATRScalingFactorSpliterator extends Spliterators.AbstractSpliterator<ShiftedMinuteDataWithScalingFactor> {

    private final Iterator<ShiftedMinuteData> sourceIterator;
    private final ATRCalculator shortATRCalc;
    private final ATRCalculator longATRCalc;
    private final BigDecimal alpha;
    private final BigDecimal oneMinusAlpha;
    private final int calculationScale;

    public ATRScalingFactorSpliterator(Iterator<ShiftedMinuteData> sourceIterator,
                                       int shortPeriod, int longPeriod, BigDecimal alpha, int calculationScale) {
        super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
        this.sourceIterator = sourceIterator;
        this.shortATRCalc = new ATRCalculator(shortPeriod);
        this.longATRCalc = new ATRCalculator(longPeriod);
        this.alpha = alpha;
        this.oneMinusAlpha = BigDecimal.ONE.subtract(alpha);
        this.calculationScale = calculationScale;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ShiftedMinuteDataWithScalingFactor> action) {
        if (!sourceIterator.hasNext()) {
            return false;
        }

        ShiftedMinuteData sd = sourceIterator.next();
        // Update ATR calculators
        shortATRCalc.addBar(sd.high(), sd.low(), sd.close());
        longATRCalc.addBar(sd.high(), sd.low(), sd.close());

        BigDecimal atrShort = shortATRCalc.getATR();
        BigDecimal atrLong = longATRCalc.getATR();

        BigDecimal scalingFactor = BigDecimal.ONE; // Default to null

        // Calculate ratio and factor only if both ATRs are available and long ATR is non-zero
        if (atrShort != null && atrLong != null) {
            if (atrLong.compareTo(BigDecimal.ZERO) != 0) {
                BigDecimal atrRatio = atrShort.divide(atrLong, calculationScale, RoundingMode.HALF_UP);
                // factor = alpha * ratio + (1 - alpha)
                scalingFactor = alpha.multiply(atrRatio).add(oneMinusAlpha)
                        .setScale(calculationScale, RoundingMode.HALF_UP);
            }
        }

        action.accept(new ShiftedMinuteDataWithScalingFactor(sd, scalingFactor));
        return true;
    }

    // trySplit returns null because this operation must be sequential due to ATR state
    @Override
    public Spliterator<ShiftedMinuteDataWithScalingFactor> trySplit() {
        return null;
    }
}