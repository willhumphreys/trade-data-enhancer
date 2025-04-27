package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CopyDailyATRToMinute {

    // Use UTC for consistency when converting epoch seconds to dates
    private static final ZoneOffset ZONE_ID = ZoneOffset.UTC;

    public static void copyDailyATRToMinute(Path minuteDataPath, Path dailyDataPath, Path atrOutputPath) throws IOException {
        log.info("Copying ATR values from daily data ({}) to minute data ({}), outputting to {}",
                dailyDataPath, minuteDataPath, atrOutputPath);

        // Step 1: Read daily data and build a sorted map from LocalDate to ATR
        TreeMap<LocalDate, BigDecimal> dailyATRMap = buildDailyATRMap(dailyDataPath); // Changed to TreeMap
        log.info("Built sorted map with {} daily ATR entries.", dailyATRMap.size());

        if (dailyATRMap.isEmpty()) {
            throw new IllegalArgumentException("Daily ATR map is empty. Cannot proceed.");
        }

        // Step 2: Read minute data stream
        BitcoinLongDataReader reader = new BitcoinLongDataReader();
        Stream<ShiftedData> minuteDataStream = reader.readFile(minuteDataPath);

        // Step 3: Process minute data, look up ATR by date (with forward fill), and prepare output lines
        List<String> outputLines = new ArrayList<>();
        outputLines.add("Timestamp,open,high,low,close,volume,atr"); // Add header

        AtomicInteger rowNumber = new AtomicInteger(1); // For error reporting
        AtomicReference<LocalDate> lastLoggedDate = new AtomicReference<>(null); // Track the last date processed for logging
        AtomicInteger daysProcessed = new AtomicInteger(0); // Count processed days for logging

        minuteDataStream.forEach(minute -> {
            int currentRow = rowNumber.getAndIncrement();
            long minuteTimestamp = minute.timestamp();
            Instant minuteInstant = Instant.ofEpochSecond(minuteTimestamp);
            LocalDate minuteDate = minuteInstant.atZone(ZONE_ID).toLocalDate(); // Get the date in UTC

            // Log progress every 100 unique dates processed
            if (!minuteDate.equals(lastLoggedDate.get())) {
                lastLoggedDate.set(minuteDate);
                int currentDayCount = daysProcessed.incrementAndGet();
                if (currentDayCount % 100 == 0) {
                    log.info("Processed {} unique dates. Current date: {}", currentDayCount, minuteDate.format(DateTimeFormatter.ISO_DATE));
                }
            }

            // Look up the ATR value for the minute's date
            BigDecimal atrValue = dailyATRMap.get(minuteDate);
            LocalDate atrSourceDate = minuteDate; // Track the date the ATR value actually came from

            if (atrValue == null) {
                // ATR not found for the exact date, try forward fill
                Map.Entry<LocalDate, BigDecimal> floorEntry = dailyATRMap.floorEntry(minuteDate.minusDays(1));

                if (floorEntry != null) {
                    // Found a previous date's ATR, use it (forward fill)
                    atrValue = floorEntry.getValue();
                    atrSourceDate = floorEntry.getKey();
                    log.warn("Missing daily ATR for date: {}. Forward-filling using value from date: {} for minute row: {} (Timestamp: {})",
                            minuteDate.format(DateTimeFormatter.ISO_DATE),
                            atrSourceDate.format(DateTimeFormatter.ISO_DATE),
                            currentRow, minuteTimestamp);
                } else {
                    // No ATR found for the date, and no previous date exists in the map.
                    // This happens if the minute data starts before the first entry in the daily data.
                    String dateString = minuteDate.format(DateTimeFormatter.ISO_DATE);
                    String dayOfWeek = minuteDate.getDayOfWeek().toString();
                    String errorMessage = String.format(
                            "Missing daily ATR data for date: %s (%s) and no prior date found for forward-fill. Required by minute data row: %d (Timestamp: %d)",
                            dateString, dayOfWeek, currentRow, minuteTimestamp
                    );
                    log.error("{}, Failing minute data row content: {}", errorMessage, minute);
                    // Throw exception as forward fill is not possible
                    throw new NoSuchElementException(errorMessage);
                }
            }

            // Format the output line
            outputLines.add(
                    String.format("%d,%d,%d,%d,%d,%.8f,%s", // Use %.8f for volume, %s for ATR plain string
                            minuteTimestamp,
                            minute.open(),
                            minute.high(),
                            minute.low(),
                            minute.close(),
                            minute.volume(), // Assuming volume is double in ShiftedData
                            atrValue.toPlainString())
            );
        });

        // Step 4: Write the updated minute data to the output file
        Files.write(atrOutputPath, outputLines);
        log.info("Minute data successfully updated with daily ATR values (using forward-fill where necessary). Output written to {}", atrOutputPath);
    }

    /**
     * Reads the daily data file and creates a sorted map from LocalDate (UTC) to the ATR value.
     * It handles potential duplicate dates by keeping the last encountered entry for a date.
     *
     * @param dailyDataPath Path to the daily data CSV file.
     * @return A TreeMap where keys are LocalDates (UTC) and values are ATR BigDecimals, sorted by date.
     * @throws IOException If there's an error reading the file.
     */
    private static TreeMap<LocalDate, BigDecimal> buildDailyATRMap(Path dailyDataPath) throws IOException { // Return TreeMap
        List<String> dailyLines = Files.readAllLines(dailyDataPath);
        if (dailyLines.size() < 2) { // Check for header + at least one data row
            log.warn("Daily data file is empty or contains only a header: {}. Returning empty map.", dailyDataPath);
            return new TreeMap<>(); // Return empty TreeMap
        }

        // Assume header is present and skip it (index 0)
        // Assume Timestamp is column 0, ATR is column 6
        try {
            // Collect into a TreeMap to ensure sorting by date
            return dailyLines.stream()
                    .skip(1) // Skip header row
                    .map(line -> line.split(","))
                    .filter(parts -> parts.length > 6) // Basic check for enough columns
                    .collect(Collectors.toMap(
                            parts -> {
                                // Parse timestamp (handle potential double format)
                                long epochSeconds = (long) Double.parseDouble(parts[0]);
                                Instant instant = Instant.ofEpochSecond(epochSeconds);
                                return instant.atZone(ZONE_ID).toLocalDate(); // Convert to LocalDate in UTC
                            },
                            parts -> new BigDecimal(parts[6]), // Parse ATR value
                            (existingValue, newValue) -> newValue, // If duplicate date, keep the last one found
                            TreeMap::new // Specify TreeMap as the map implementation
                    ));
        } catch (NumberFormatException e) {
            log.error("Failed to parse number in daily data file: {}", dailyDataPath, e);
            throw new IOException("Failed to parse daily data file due to number format error.", e);
        } catch (ArrayIndexOutOfBoundsException e) {
            log.error("Failed to access expected column index in daily data file: {}", dailyDataPath, e);
            throw new IOException("Failed to parse daily data file due to unexpected row format.", e);
        }
    }
}