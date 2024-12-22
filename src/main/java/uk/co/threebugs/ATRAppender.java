package uk.co.threebugs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A utility class for appending the Average True Range (ATR) to a stream of minute-level financial data.
 * ATR is a technical analysis indicator that measures market volatility.
 */
public class ATRAppender {

    public void appendATR(Stream<MinuteData> data, int atrWindow1, Path outputPath) {

        writeStreamToFile(appendATR(data, atrWindow1), outputPath);

    }

    Stream<MinuteDataWithATR> appendATR(Stream<MinuteData> data, int atrWindow1) {
        // Use an ArrayList to store the previous close and calculated ATR values
        List<Double> previousCloses = new ArrayList<>();
        List<Double> atrValues = new ArrayList<>();


        return data.map(minuteData -> getMinuteDataWithATR(minuteData, previousCloses, atrWindow1, atrValues));

    }


    public void writeStreamToFile(Stream<MinuteDataWithATR> stream, Path outputPath) {
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            writer.write("Timestamp,Open,High,Low,Close,Volume,ATR");
            writer.newLine();

            stream.forEach(dataWithATR -> {
                try {
                    writer.write(formatDataEntry(dataWithATR));
                    writer.newLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw new RuntimeException(e.getCause());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String formatDataEntry(MinuteDataWithATR entry) {
        MinuteData data = entry.minuteData();
        return data.timestamp() + "," +
                data.open() + "," +
                data.high() + "," +
                data.low() + "," +
                data.close() + "," +
                data.volume() + "," +
                entry.atr();
    }

    private MinuteDataWithATR getMinuteDataWithATR(MinuteData minuteData, List<Double> previousCloses, int atrWindow, List<Double> atrValues) {
        // Calculate True Range for current row
        double previousClose = previousCloses.isEmpty() ? minuteData.close() : previousCloses.getLast();
        double tr = Math.max(
                minuteData.high() - minuteData.low(),
                Math.max(
                        Math.abs(minuteData.high() - previousClose),
                        Math.abs(minuteData.low() - previousClose))
        );

        // Add the close price to the list
        previousCloses.add(minuteData.close());

        // Calculate ATR once we have enough data for the window size
        double atr = 0.0;
        if (previousCloses.size() >= atrWindow) {
            atrValues.add(tr); // Add TR to ATR calculation

            if (atrValues.size() > atrWindow) {
                // Keep only the last 'atrWindow' TR values
                atrValues.removeFirst();
            }

            atr = atrValues.stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0.0); // Calculate the average (ATR)
        }

        return new MinuteDataWithATR(minuteData, atr);
    }
}
