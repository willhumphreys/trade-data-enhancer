package uk.co.threebugs;

import lombok.Builder;

/**
 * Represents minute-level financial data where all numerical values are decimal-shifted
 * and stored as `long` for precision and consistency.
 */
@Builder
public record ShiftedMinuteData(long timestamp, long open, long high, long low, long close, double volume) {
}