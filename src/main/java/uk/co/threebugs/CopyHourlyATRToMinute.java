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
public class CopyHourlyATRToMinute {

    public static void copyHourlyATRToMinute(Path minuteData, Path hourlyData, Path atrOutput) throws IOException {
        log.info("Copying ATR values from hourly data to minute data...");

        // Read hourly data lines (use Files.lines() to process as a stream)
        List<String> hourlyLines = Files.readAllLines(hourlyData);

        // Extract the header and parse hourly data
        if (hourlyLines.isEmpty()) {
            throw new IllegalArgumentException("Hourly data file is empty");
        }
        List<String> dataLines = hourlyLines.subList(1, hourlyLines.size()); // Skip header

        // Create a map of hourly ATR values (Key: Hourly Timestamp, Value: ATR)
        // Assuming the hourly file has the format: Timestamp,Open,High,Low,Close,Volume,ATR
        Map<Long, Long> hourlyATRMap = dataLines.stream()
                .map(line -> line.split(","))
                .collect(Collectors.toMap(
                        parts -> (long) Double.parseDouble(parts[0]),   // Hourly Timestamp
                        parts -> Long.parseLong(parts[6])    // ATR value from hourly data
                ));

        // Read minute data using BitcoinLongDataReader
        BitcoinLongDataReader reader = new BitcoinLongDataReader();
        Stream<ShiftedData> minuteDataStream = reader.readFile(minuteData);

        // Map ATR values to the corresponding minute data
        List<String> updatedMinuteData = new ArrayList<>();
        updatedMinuteData.add("Timestamp,open,high,low,close,volume,atr"); // CSV Header

        minuteDataStream.forEach(minute -> {
            long minuteTimestamp = minute.timestamp();
            long associatedHourTimestamp = (minuteTimestamp / 3600) * 3600; // Align to hourly start
            long atrValue = hourlyATRMap.getOrDefault(associatedHourTimestamp, -1L); // Default to -1 if no ATR value
            updatedMinuteData.add(
                    String.format("%d,%d,%d,%d,%d,%s,%d",
                            minuteTimestamp, minute.open(), minute.high(),
                            minute.low(), minute.close(), minute.volume(), atrValue)
            );
        });

        // Write the updated minute data with ATR values back to the file
        Files.write(atrOutput, updatedMinuteData);
        log.info("Minute data successfully updated with ATR values copied from hourly data.");
    }
}