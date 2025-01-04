package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Appends Average True Range (ATR) to a stream of ShiftedMinuteData objects.
 */
@Slf4j
public class ATRAppender {

    private long largestATR = 0L;
    private long smallestATR = Long.MAX_VALUE;

    private int processedCount = 0;

    public void appendATR(Stream<ShiftedMinuteData> data, int atrWindow, Path outputPath) {
        writeStreamToFile(appendATR(data, atrWindow), outputPath);
    }

    Stream<ShiftedMinuteDataWithATR> appendATR(Stream<ShiftedMinuteData> data, int atrWindow) {
        // Use a LinkedList for efficient sliding window operations
        List<Long> previousCloses = new LinkedList<>();
        List<Long> atrValues = new LinkedList<>();

        return data.map(minuteData -> getMinuteDataWithATR(minuteData, previousCloses, atrWindow, atrValues));
    }

    /**
     * Writes ATR-enhanced data to the specified file.
     */
    public void writeStreamToFile(Stream<ShiftedMinuteDataWithATR> stream, Path outputPath) {
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            writer.write("Timestamp,open,high,low,close,volume,atr");
            writer.newLine();

            stream.forEach(dataWithATR -> {
                try {
                    writer.write(formatDataEntry(dataWithATR));
                    writer.newLine();
                    processedCount++;
                    if (dataWithATR.atr() > largestATR) {
                        largestATR = dataWithATR.atr();
                    }
                    if (dataWithATR.atr() < smallestATR) {
                        smallestATR = dataWithATR.atr();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw new RuntimeException(e.getCause());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("Added ATR to {} rows. Largest ATR: {} Smallest ATR: {}", processedCount, largestATR, smallestATR);
    }

    /**
     * Formats a row for output.
     */
    private String formatDataEntry(ShiftedMinuteDataWithATR entry) {
        ShiftedMinuteData data = entry.minuteData();

        // Directly write long values (no conversion or formatting needed)
        return data.timestamp() + "," +
                data.open() + "," +
                data.high() + "," +
                data.low() + "," +
                data.close() + "," +
                data.volume() + "," +
                entry.atr();
    }

    /**
     * Handles ATR calculation for ShiftedMinuteData.
     */
    private ShiftedMinuteDataWithATR getMinuteDataWithATR(ShiftedMinuteData minuteData, List<Long> previousCloses, int atrWindow, List<Long> atrValues) {
        // Use long values directly
        long open = minuteData.open();
        long high = minuteData.high();
        long low = minuteData.low();
        long close = minuteData.close();

        long previousClose = previousCloses.isEmpty() ? close : previousCloses.get(previousCloses.size() - 1);
        long tr = Math.max(high - low, Math.max(Math.abs(high - previousClose), Math.abs(low - previousClose)));

        // Add the close price to the sliding window
        previousCloses.add(close);

        // Remove the oldest close when the list exceeds the ATR window size
        if (previousCloses.size() > atrWindow) {
            previousCloses.remove(0);
        }

        // Calculate ATR when enough data is available
        long atr = -1L;
        if (previousCloses.size() >= atrWindow) {
            atrValues.add(tr);

            // Keep only the last 'atrWindow' TR values
            if (atrValues.size() > atrWindow) {
                atrValues.remove(0);
            }

            // Calculate the average of TR values
            atr = atrValues.stream()
                    .mapToLong(Long::longValue)
                    .sum() / atrValues.size();
        }

        // Return ATR data
        return new ShiftedMinuteDataWithATR(minuteData, atr);
    }
}