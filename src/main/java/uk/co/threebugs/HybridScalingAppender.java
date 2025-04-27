package uk.co.threebugs;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.indicators.ATRIndicator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class HybridScalingAppender {

    private static final int SCALE = 8; // Scale for BigDecimal precision
    private final int shortATRPeriod;
    private final int longATRPeriod;
    private final BigDecimal alpha;
    private final TimeFrame timeFrame;

    public HybridScalingAppender(int shortATRPeriod, int longATRPeriod, double alpha, TimeFrame timeFrame) {
        this.timeFrame = timeFrame;
        if (alpha < 0 || alpha > 1) throw new IllegalArgumentException("Alpha must be between 0 and 1");
        this.shortATRPeriod = shortATRPeriod;
        this.longATRPeriod = longATRPeriod;
        this.alpha = BigDecimal.valueOf(alpha);
    }

    /**
     * Processes a stream of ShiftedData to append hybrid scaling factors and other metrics.
     *
     * @param data The stream of ShiftedData records.
     * @return A stream of ShiftedDataWithHybridScaling records.
     */
    public Stream<ShiftedDataWithHybridScaling> appendScalingFactors(Stream<ShiftedData> data) {
        List<ShiftedData> dataList = data.collect(Collectors.toList());
        if (dataList.isEmpty()) {
            throw new IllegalStateException("data was empty");
        }
        BigDecimal baselinePrice = calculateBaselinePrice(dataList);
        List<Bar> bars = convertToBars(dataList);
        BaseBarSeries barSeries = new BaseBarSeries("daily-data", bars);

        ATRIndicator shortATRIndicator = new ATRIndicator(barSeries, shortATRPeriod);
        ATRIndicator longATRIndicator = new ATRIndicator(barSeries, longATRPeriod);

        // Use IntStream to get index easily
        return IntStream.range(0, dataList.size()).mapToObj(index -> {
            ShiftedData sd = dataList.get(index);
            BigDecimal scalingFactor;

            if (timeFrame == TimeFrame.DAILY) {
                BigDecimal price = BigDecimal.valueOf(sd.close());
                // ta4j indicators handle initial unstable values
                BigDecimal shortATR = BigDecimal.valueOf(shortATRIndicator.getValue(index).doubleValue());
                BigDecimal longATR = BigDecimal.valueOf(longATRIndicator.getValue(index).doubleValue());
                scalingFactor = calculateHybridScalingFactor(shortATR, longATR, price, baselinePrice);
            } else {
                scalingFactor = BigDecimal.ONE;
            }

            // Log progress every 1000 records
            // Check (index + 1) to log after processing the 1000th, 2000th, etc. record
            if ((index + 1) % 1000 == 0) {
                long epochSeconds = sd.timestamp();
                Instant instant = Instant.ofEpochSecond(epochSeconds);
                // Convert to ZonedDateTime using the system default timezone
                ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
                // ZonedDateTime.toString() produces ISO-8601 format
                log.info("Processed record {} - Timestamp: {}", index + 1, zdt);
            }

            return new ShiftedDataWithHybridScaling(sd.timestamp(), sd.open(), sd.high(), sd.low(), sd.close(), sd.volume(), scalingFactor);
        });
    }

    private List<Bar> convertToBars(List<ShiftedData> dataList) {
        List<Bar> bars = new ArrayList<>();
        for (ShiftedData sd : dataList) {
            ZonedDateTime endTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(sd.timestamp()), ZoneId.systemDefault());
            bars.add(new BaseBar(Duration.ofMinutes(1), endTime, (double) sd.open(), (double) sd.high(), (double) sd.low(), (double) sd.close(), sd.volume()));
        }
        return bars;
    }

    private BigDecimal calculateBaselinePrice(List<ShiftedData> dataList) {
        return dataList.stream().map(sd -> BigDecimal.valueOf(sd.close())).reduce(BigDecimal.ZERO, BigDecimal::add).divide(BigDecimal.valueOf(dataList.size()), SCALE, RoundingMode.HALF_UP);
    }

    public BigDecimal calculateHybridScalingFactor(BigDecimal shortATR, BigDecimal longATR, BigDecimal price, BigDecimal baselinePrice) {
        if (shortATR.compareTo(BigDecimal.ZERO) <= 0 || longATR.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal normalizedATRRatio = shortATR.divide(price, SCALE, RoundingMode.HALF_UP).divide(longATR.divide(baselinePrice, SCALE, RoundingMode.HALF_UP), SCALE, RoundingMode.HALF_UP);
        BigDecimal absoluteATRRatio = shortATR.divide(longATR, SCALE, RoundingMode.HALF_UP);
        return alpha.multiply(normalizedATRRatio).add(BigDecimal.ONE.subtract(alpha).multiply(absoluteATRRatio));
    }

    public void writeStreamToFile(Stream<ShiftedDataWithHybridScaling> stream, Path outputPath) {
        List<String> outputLines = new ArrayList<>();
        outputLines.add("Timestamp,Open,High,Low,Close,Volume,HybridScalingFactor");

        stream.forEach(entry -> outputLines.add(formatDataEntry(entry)));

        try {
            Files.write(outputPath, outputLines);
            log.info("Data with hybrid scaling factors written to {}", outputPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write data to file", e);
        }
    }

    private String formatDataEntry(ShiftedDataWithHybridScaling entry) {
        return String.format("%d,%d,%d,%d,%d,%.2f,%.8f", entry.timestamp(), entry.open(), entry.high(), entry.low(), entry.close(), entry.volume(), entry.hybridScalingFactor());
    }

    @Builder
    public record ShiftedDataWithHybridScaling(long timestamp, long open, long high, long low, long close,
                                               double volume, BigDecimal hybridScalingFactor) {
    }
}
