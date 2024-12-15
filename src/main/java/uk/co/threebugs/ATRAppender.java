package uk.co.threebugs;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A utility class for appending the Average True Range (ATR) to a stream of minute-level financial data.
 * ATR is a technical analysis indicator that measures market volatility.
 */
public class ATRAppender {


    public Stream<MinuteDataWithATR> appendATR(Stream<MinuteData> data, int atrWindow1) {
        // Use an ArrayList to store the previous close and calculated ATR values
        List<Double> previousCloses = new ArrayList<>();
        List<Double> atrValues = new ArrayList<>();


        return data.map(minuteData -> getMinuteDataWithATR(minuteData, previousCloses, atrWindow1, atrValues));

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
