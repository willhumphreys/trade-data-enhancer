package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CopyDailyATRToMinute {

    // Define a default value for missing ATR, or handle null appropriately
    private static final BigDecimal DEFAULT_ATR = BigDecimal.valueOf(-1); // Or consider using null

    public static void copyDailyATRToMinute(Path minuteData, Path dailyData, Path atrOutput) throws IOException {
        log.info("Copying ATR values from daily data to minute data...");

        // Read daily data lines
        List<String> dailyLines = Files.readAllLines(dailyData);
        if (dailyLines.isEmpty()) {
            throw new IllegalArgumentException("Daily data file is empty: " + dailyData);
        }

        // Extract the header and sublist of data lines
        List<String> dataLines = dailyLines.subList(1, dailyLines.size());

        // Build a map of daily ATR values (Timestamp -> ATR)
        // Assumes the daily file has the format: Timestamp,Open,High,Low,Close,Volume,ATR
        Map<Long, BigDecimal> dailyATRMap = dataLines.stream()
                .map(line -> line.split(","))
                .filter(parts -> parts.length > 6 && !parts[6].trim().isEmpty()) // Ensure ATR column exists and is not empty
                .collect(Collectors.toMap(
                        parts -> (long) Double.parseDouble(parts[0]), // Daily Timestamp (consider parsing safely)
                        parts -> new BigDecimal(parts[6]),           // ATR value as BigDecimal
                        (existing, replacement) -> existing          // Handle duplicate keys if necessary
                ));

        // Read minute data using BitcoinLongDataReader
        BitcoinLongDataReader reader = new BitcoinLongDataReader();
        Stream<ShiftedMinuteData> minuteDataStream = reader.readFile(minuteData);

        // Create a new list to store updated minute data, including a CSV header
        List<String> updatedMinuteData = new ArrayList<>();
        // Update header to reflect the column name
        updatedMinuteData.add("Timestamp,open,high,low,close,volume,scalingFactor");

        minuteDataStream.forEach(minute -> {
            long minuteTimestamp = minute.timestamp();
            // Align to daily start (each day is 86400 seconds)
            long associatedDayTimestamp = (minuteTimestamp / 86400L) * 86400L;
            // Get ATR, use default if not found
            BigDecimal atrValue = dailyATRMap.getOrDefault(associatedDayTimestamp, DEFAULT_ATR);

            // Format the output string, handling potential null or default ATR
            String atrString = (atrValue != null) ? atrValue.toPlainString() : ""; // Use empty string for null/default if desired

            updatedMinuteData.add(
                    String.format("%d,%d,%d,%d,%d,%.2f,%s", // Use %.2f for volume, %s for ATR string
                            minuteTimestamp, minute.open(), minute.high(),
                            minute.low(), minute.close(), minute.volume(), atrString)
            );
        });

        // Write the updated minute data with ATR values to the output file
        Files.write(atrOutput, updatedMinuteData);
        log.info("Minute data successfully updated with daily ATR values written to {}", atrOutput);
    }
}