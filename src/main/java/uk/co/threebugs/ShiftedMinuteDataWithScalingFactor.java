package uk.co.threebugs;

import java.math.BigDecimal;

/**
 * Holds the original minute data along with the calculated scaling factor.
 */
public record ShiftedMinuteDataWithScalingFactor(ShiftedMinuteData minuteData, BigDecimal scalingFactor) {
}