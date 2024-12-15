package uk.co.threebugs;

import lombok.Builder;

import java.time.LocalDateTime;

/**
 * A record representing a single data entry with attributes such as timestamp,
 * open, high, low, close, and volume.
 */
@Builder
public record DataEntry(
        LocalDateTime timestamp,
        double open,
        double high,
        double low,
        double close,
        double volume
) {
}