package uk.co.threebugs;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Calculates a scaling factor based on the ratio of short-term ATR to long-term ATR
 * for a stream of ShiftedMinuteData and returns a stream containing the original data
 * along with the calculated scaling factor.
 */
@Slf4j
@RequiredArgsConstructor
public class ATRScalingFactorAppender {

    private static final int CALCULATION_SCALE = 8; // Precision for intermediate calculations


    /**
     * Formats a ShiftedMinuteDataWithScalingFactor record as a CSV string.
     *
     * @param entry The ShiftedMinuteDataWithScalingFactor entry.
     * @return A formatted CSV string.
     */
    private String formatDataEntry(ShiftedMinuteDataWithScalingFactor entry) {
        // Assuming ShiftedMinuteData fields are long/int except volume (double)
        // Handle null scalingFactor
        String scalingFactorStr = entry.scalingFactor() != null
                ? entry.scalingFactor().setScale(CALCULATION_SCALE, RoundingMode.HALF_UP).toPlainString()
                : ""; // Write empty string if null

        return String.format("%d,%d,%d,%d,%d,%.2f,%s",
                entry.minuteData().timestamp(),
                entry.minuteData().open(),
                entry.minuteData().high(),
                entry.minuteData().low(),
                entry.minuteData().close(),
                entry.minuteData().volume(),
                scalingFactorStr);
    }

    public Stream<ShiftedMinuteDataWithScalingFactor> appendScalingFactor(Stream<ShiftedMinuteData> data, int shortPeriod, int longPeriod, BigDecimal alpha) {
        if (alpha == null || alpha.compareTo(BigDecimal.ZERO) < 0 || alpha.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException("Alpha must be between 0 and 1, inclusive.");
        }

        log.info("Calculating ATR scaling factor (short={}, long={}, alpha={}). Processing stream sequentially.",
                shortPeriod, longPeriod, alpha.toPlainString());

        // Use the custom Spliterator to process the stream without intermediate collection
        Iterator<ShiftedMinuteData> iterator = data.iterator();
        Spliterator<ShiftedMinuteDataWithScalingFactor> spliterator = new ATRScalingFactorSpliterator(
                iterator, shortPeriod, longPeriod, alpha, CALCULATION_SCALE
        );

        return StreamSupport.stream(spliterator, false);
    }


    // Update writeStreamToFile for true streaming output
    public void writeStreamToFile(Stream<ShiftedMinuteDataWithScalingFactor> stream, Path outputPath) {
        // Use try-with-resources for BufferedWriter
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            // Write header
            writer.write("Timestamp,open,high,low,close,volume,scalingFactor");
            writer.newLine();

            // Process the stream and write each line directly
            stream.map(this::formatDataEntry)
                    .forEachOrdered(line -> { // Use forEachOrdered to maintain sequence
                        try {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            // Wrap IOExceptions from lambda into UncheckedIOException
                            throw new UncheckedIOException("Failed writing line to " + outputPath, e);
                        }
                    });

            log.info("ATR scaling factor data written to {}", outputPath);

        } catch (IOException e) {
            // Catch exceptions from opening the writer or outer scope
            throw new UncheckedIOException("Failed to write ATR scaling factor data to file: " + outputPath, e);
        } catch (UncheckedIOException e) {
            // Catch exceptions wrapped within the forEachOrdered lambda
            log.error("Error during stream processing for file writing: {}", e.getMessage());
            throw e; // Re-throw
        }
    }
}