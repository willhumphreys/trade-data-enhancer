package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeries;
import org.ta4j.core.indicators.ATRIndicator;
import org.ta4j.core.indicators.helpers.TRIndicator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Appends Average True Range (ATR) to a stream of ShiftedMinuteData objects using TA4J.
 */
@Slf4j
public class ATRAppender {

    /**
     * Enhances minute data with ATR values computed over the given window.
     *
     * @param data      A stream of ShiftedMinuteData.
     * @param atrWindow Number of periods to use in ATR calculation.
     * @return A stream of ShiftedMinuteDataWithATR containing the ATR values.
     */
    public Stream<ShiftedMinuteDataWithATR> appendATR(Stream<ShiftedMinuteData> data, int atrWindow) {
        // Collect all minute data into a list for conversion and for computing ATR values.
        List<ShiftedMinuteData> minuteDataList = data.collect(Collectors.toList());

        // Convert the list of minute data into a list of TA4J Bars.
        List<Bar> bars = new ArrayList<>(minuteDataList.size());
        for (ShiftedMinuteData sd : minuteDataList) {
            // Convert the timestamp (assumed seconds) to ZonedDateTime.
            ZonedDateTime endTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(sd.timestamp()), ZoneId.systemDefault());
            // Convert ShiftedMinuteData to a TA4J bar. Duration here is set to 1 minute.
            Bar bar = new BaseBar(
                    Duration.ofMinutes(1),
                    endTime,
                    (double) sd.open(),
                    (double) sd.high(),
                    (double) sd.low(),
                    (double) sd.close(),
                    sd.volume()
            );
            bars.add(bar);
        }

        // Create a TA4J TimeSeries from the bars.
        TRIndicator timeSeries = new TRIndicator(new BaseBarSeries("minute-data", bars));

        // Create the ATR indicator from TA4J.
        ATRIndicator atrIndicator = new ATRIndicator(timeSeries, atrWindow);

        // Map each minute data entry with its corresponding ATR value.
        // It is assumed that ShiftedMinuteDataWithATR is a record with the following structure:
        //   record ShiftedMinuteDataWithATR(long timestamp, long open, long high, long low, long close, double volume, double atr)
        List<ShiftedMinuteDataWithATR> dataWithATR = new ArrayList<>(minuteDataList.size());
        for (int i = 0; i < minuteDataList.size(); i++) {
            ShiftedMinuteData sd = minuteDataList.get(i);
            long atrValue = atrIndicator.getValue(i).longValue();

            dataWithATR.add(new ShiftedMinuteDataWithATR(sd, atrValue));
        }

        log.info("Successfully appended ATR values using TA4J.");
        return dataWithATR.stream();
    }

    /**
     * Writes ATR-enhanced data to the specified file.
     *
     * @param stream     The stream of ShiftedMinuteDataWithATR records.
     * @param outputPath The output file path.
     */
    public void writeStreamToFile(Stream<ShiftedMinuteDataWithATR> stream, Path outputPath) {
        // Prepare a list to hold CSV lines including the header.
        List<String> outputLines = new ArrayList<>();
        outputLines.add("Timestamp,open,high,low,close,volume,atr");

        // Format each entry as a CSV record.
        stream.forEach(data -> {
            String line = formatDataEntry(data);
            outputLines.add(line);
        });

        try {
            Files.write(outputPath, outputLines);
            log.info("ATR-enhanced minute data written to {}", outputPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write ATR-enhanced data to file", e);
        }
    }

    /**
     * Formats a ShiftedMinuteDataWithATR record as a CSV string.
     *
     * @param entry The data entry with ATR.
     * @return A CSV-formatted string.
     */
    private String formatDataEntry(ShiftedMinuteDataWithATR entry) {
        return String.format("%d,%d,%d,%d,%d,%.2f,%d",
                entry.minuteData().timestamp(),
                entry.minuteData().open(),
                entry.minuteData().high(),
                entry.minuteData().low(),
                entry.minuteData().close(),
                entry.minuteData().volume(),
                entry.atr());
    }
}