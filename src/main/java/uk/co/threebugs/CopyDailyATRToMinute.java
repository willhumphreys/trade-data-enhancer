package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CopyDailyATRToMinute {

    public static void copyDailyATRToMinute(Path minuteData, Path dailyData, Path atrOutput) throws IOException {
        log.info("Copying ATR values from daily data to minute data...");

        // Read daily data lines (using Files.readAllLines to process as a list)
        List<String> dailyLines = Files.readAllLines(dailyData);
        if (dailyLines.isEmpty()) {
            throw new IllegalArgumentException("Daily data file is empty");
        }

        // Extract the header and sublist of data lines
        List<String> dataLines = dailyLines.subList(1, dailyLines.size());

        // Build a map of daily ATR values
        // Assumes the daily file has the format: Timestamp,Open,High,Low,Close,Volume,ATR
        Map<Long, Long> dailyATRMap = dataLines.stream()
                .map(line -> line.split(","))
                .collect(Collectors.toMap(
                        parts -> (long) Double.parseDouble(parts[0]),   // Daily Timestamp
                        parts -> Long.parseLong(parts[6])                 // ATR value from daily data
                ));

        // Read minute data using BitcoinLongDataReader
        BitcoinLongDataReader reader = new BitcoinLongDataReader();
        Stream<ShiftedMinuteData> minuteDataStream = reader.readFile(minuteData);

        // Create a new list to store updated minute data, including a CSV header
        List<String> updatedMinuteData = new ArrayList<>();
        updatedMinuteData.add("Timestamp,open,high,low,close,volume,atr");

        minuteDataStream.forEach(minute -> {
            long minuteTimestamp = minute.timestamp();
            // Align to daily start (each day is 86400 seconds)
            long associatedDayTimestamp = (minuteTimestamp / 86400L) * 86400L;
            long atrValue = dailyATRMap.getOrDefault(associatedDayTimestamp, -1L); // Default to -1 if not found

            updatedMinuteData.add(
                    String.format("%d,%d,%d,%d,%d,%s,%d",
                            minuteTimestamp, minute.open(), minute.high(),
                            minute.low(), minute.close(), minute.volume(), atrValue)
            );
        });

        // Write the updated minute data with ATR values to the output file
        Files.write(atrOutput, updatedMinuteData);
        log.info("Minute data successfully updated with ATR values copied from daily data.");
    }
}