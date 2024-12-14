package uk.co.threebugs;

import lombok.Builder;

@Builder
public record MinuteData(long timestamp, double open, double high, double low, double close, double volume) {
}
