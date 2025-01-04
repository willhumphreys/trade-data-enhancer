package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException {
        var options = new Options();
        options.addOption("w", "window", true, "ATR window size (e.g., 14 periods)");
        options.addOption("f", "file", true, "Minute data file name (e.g., spx-1m-btmF.csv)");
        options.addOption("h", "hourly", true, "Hourly data file name (e.g., spx-1h-btmF.csv)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            int atrWindowDefault = 14 * 24; // 336 for 14 days

            // Parse ATR window size (default: 14 periods)
            var atrWindow = Integer.parseInt(cmd.getOptionValue("w", String.valueOf(atrWindowDefault)));

            // File names for minute and hourly data
            var minuteDataFile = cmd.getOptionValue("f", "btcusd_1-min_data.csv");
            var hourlyDataFile = cmd.getOptionValue("h", "btcusd_1-hour_data.csv");

            // Define input/output directories
            var dataDirectory = Path.of("data");
            var inputDirectory = dataDirectory.resolve("input");
            var outputDirectory = dataDirectory.resolve("output");

            deleteAllFilesInDirectory(outputDirectory);

            // Prepare paths for data files
            var minuteDataPath = inputDirectory.resolve(minuteDataFile);
            var hourlyDataPath = inputDirectory.resolve(hourlyDataFile);

            // Check if the hourly file exists
            if (Files.notExists(hourlyDataPath)) {
                log.info("Hourly file '{}' not found. Generating it from the minute file '{}'.", hourlyDataFile, minuteDataFile);

                new HourlyFileGenerator().generateHourlyFileFromMinuteFile(minuteDataPath, hourlyDataPath);
            }

            // Output paths for intermediate processing
            var processedMinutePaths = new ProcessedPaths(minuteDataFile, outputDirectory);
            var processedHourlyPaths = new ProcessedPaths(hourlyDataFile, outputDirectory);

            log.info("Starting data processing for Minute Data: {} and Hourly Data: {}", minuteDataFile, hourlyDataFile);

            // Process minute data (validation, decimal shift, and timestamp check)
            processMinuteData(minuteDataPath, processedMinutePaths);

            // Process hourly data (validation, decimal shift, and timestamp check)
            processHourlyData(hourlyDataPath, processedHourlyPaths, atrWindow);


            // Combine hourly and minute data for integrity check
            performDataIntegrityCheck(processedMinutePaths.decimalShifted, processedMinutePaths.hourlyChecked, processedHourlyPaths.hourlyAtrOutput, processedMinutePaths.atrOutput);

            // Additional processing for ATR and formatting
            performAdditionalProcessing(processedMinutePaths);

        } catch (ParseException e) {
            log.error("Error parsing command line arguments: {}", e.getMessage());
            new HelpFormatter().printHelp("Main", options);
        }
    }

    private static void processHourlyData(Path hourlyDataPath, ProcessedPaths paths, int atrWindow) throws IOException {
        log.info("Processing hourly data...");

        var validator = new DataValidator();
        var decimalShifter = new DecimalShifter();
        var timestampOrderChecker = new TimestampOrderChecker();

        // Step 1: Validate hourly data
        validator.validateDataFile(hourlyDataPath, paths.validated, paths.invalid);
        log.info("Hourly data validated.");

        // Step 2: Decimal shift
        Path shiftValuePath = paths.validated.getParent().resolve("decimal_shift_value.txt");
        if (!Files.exists(shiftValuePath)) {
            throw new IOException("Decimal shift value file not found. Ensure minute data is processed first.");
        }

        int decimalShift = Integer.parseInt(Files.readString(shiftValuePath).trim());
        decimalShifter.shiftDecimalPlacesWithPredefinedShift(paths.validated, paths.decimalShifted, decimalShift);
        log.info("Hourly data decimal values adjusted using predefined shift value: {}.", decimalShift);

        // Step 3: Verify timestamps
        timestampOrderChecker.checkTimestampOrder(paths.decimalShifted);
        log.info("Hourly timestamps verified to be correct.");

        // Step 4: Append ATR values
        log.info("Appending ATR values to hourly data...");
        var atrAppender = new ATRAppender();
        var reader = new BitcoinMinuteDataReader(); // Can be reused for reading hourly files


        var longReader = new BitcoinLongDataReader(); // New long value reader

        var hourlyDataLong = longReader.readFile(paths.decimalShifted); // Read hourly data


        // Write ATR-enhanced hourly stream to output
        atrAppender.appendATR(hourlyDataLong, atrWindow, paths.hourlyAtrOutput); // ATR window size is 14 by default
        log.info("ATR values successfully added to hourly data.");
    }


    private static void processMinuteData(Path minuteDataPath, ProcessedPaths paths) throws IOException {
        log.info("Processing minute data...");

        var validator = new DataValidator();
        var decimalShifter = new DecimalShifter();
        var timestampOrderChecker = new TimestampOrderChecker();

        // Step 1: Validate minute data
        validator.validateDataFile(minuteDataPath, paths.validated, paths.invalid);
        log.info("Minute data validated.");

        // Step 2: Decimal shift (returns shift value)
        int decimalShift = decimalShifter.shiftDecimalPlaces(paths.validated, paths.decimalShifted);
        log.info("Minute data decimal values shifted by {} places.", decimalShift);

        // Save the shift value for later use
        try (var writer = Files.newBufferedWriter(paths.validated.getParent().resolve("decimal_shift_value.txt"))) {
            writer.write(String.valueOf(decimalShift));
        }

        // Step 3: Verify timestamps are in order
        try {
            timestampOrderChecker.checkTimestampOrder(paths.decimalShifted);
            log.info("Minute timestamps verified to be in correct order.");
        } catch (IllegalStateException e) {
            log.error("Minute data timestamps are not in order: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Inserts missing hours into minute data.
     *
     * @param minuteData
     * @param hourlyCheckedMinuteData
     * @param hourlyAtrOutput
     * @param atrOutput
     * @throws IOException
     */
    private static void performDataIntegrityCheck(Path minuteData, Path hourlyCheckedMinuteData, Path hourlyAtrOutput, Path atrOutput) throws IOException {
        log.info("Performing data integrity check...");

        var hourlyChecker = new HourlyDataChecker();

        // Step 1: Ensure hourly entries exist in minute data
        int hourlyEntries = hourlyChecker.ensureHourlyEntries(minuteData, hourlyAtrOutput, hourlyCheckedMinuteData);
        log.info("Inserted hours: {}", hourlyEntries);

        // Step 2: Validate data integrity
        var integrityChecker = new DataIntegrityChecker();
        String integrityResult = integrityChecker.validateDataIntegrity(hourlyCheckedMinuteData, hourlyAtrOutput);

        if (!Objects.equals(integrityResult, "No issues found.")) {
            log.error("Data integrity check failed! {}", integrityResult);
            throw new IllegalStateException("Data integrity check failed! " + integrityResult);
        }
        log.info("Data integrity check passed.");

        // Step 3: Copy ATR values from hourly data to minute data
        CopyHourlyATRToMinute.copyHourlyATRToMinute(hourlyCheckedMinuteData, hourlyAtrOutput, atrOutput);
        log.info("Successfully copied ATR values from hourly data to minute data.");


    }

    private static void performAdditionalProcessing(ProcessedPaths paths) throws IOException {
        log.info("Performing additional processing steps...");

        // Step 4: Append ATR values
        //var reader = new BitcoinLongDataReader();
        //var checkedData = reader.readFile(finalMinuteData);
        //new ATRAppender().appendATR(checkedData, atrWindow, paths.atrOutput);
        //log.info("ATR values appended.");

        // Step 5: Fix date formatting
        var timestampFormatter = new TimestampFormatter();
        timestampFormatter.reformatTimestamps(paths.atrOutput, paths.timeStampFormatted);
        log.info("Timestamps reformatted.");

        // Step 6: Add file name as a column
        var fileNameAppender = new FileNameAppender();
        String fileNameWithoutExtension = paths.fileName.substring(0, paths.fileName.lastIndexOf('.'));
        fileNameAppender.addFileNameColumn(paths.timeStampFormatted, paths.nameAppended, fileNameWithoutExtension);
        log.info("File name column added.");

        // Step 7: Add weighting column
        var weightingAdder = new WeightingColumnAppender();
        var weightedOutputPath = paths.hourlyChecked.getParent().resolve("7_weighted_" + paths.fileName);
        weightingAdder.addWeightingColumn(paths.nameAppended, weightedOutputPath);
        log.info("Weighting column added. Output written to {}", weightedOutputPath);

        // Step 8: Add the 'newHour' column
        var newHourAdder = new NewHourColumnAdder();
        Path newHourPath = weightedOutputPath.getParent().resolve("8_new_hour_" + paths.fileName);
        newHourAdder.addNewHourColumn(weightedOutputPath, newHourPath);
        log.info("Added 'newHour' column. Output written to {}", newHourPath);

        // Step 9: Fix 'low' and 'high' columns
        var lowHighAdjuster = new LowHighColumnAdder();
        Path fixedLowHighPath = newHourPath.getParent().resolve("9_fixed_low_high_" + paths.fileName);
        lowHighAdjuster.addFixedLowAndHighColumns(newHourPath, fixedLowHighPath);
        log.info("Fixed 'low' and 'high' columns. Output written to {}", fixedLowHighPath);

        // Step 10: Add 'mapTime' column and check hourly trades (if necessary)
        var mapTimeAdder = new MapTimeColumnAdder();
        Path mapTimePath = fixedLowHighPath.getParent().resolve("10_map_time_" + paths.fileName);
        mapTimeAdder.addMapTimeColumnAndCheckHourlyTrades(fixedLowHighPath, mapTimePath);
        log.info("Added 'mapTime' column. Final output written to {}", mapTimePath);

        // Step 11: Add missing hourly rows
        Path missingHourlyPath = mapTimePath.getParent().resolve("11_missing_hourly_" + paths.fileName);
        addMissingHourlyRows(mapTimePath, missingHourlyPath);
        log.info("Added missing hourly rows. Final output written to {}", missingHourlyPath);

        var missingHourValidator = new MissingHourValidator();

        try {
            missingHourValidator.validateHourlyRecords(missingHourlyPath);
            log.info("No missing hourly records found.");
        } catch (IllegalStateException e) {
            log.error("Validation failed! Missing hourly records: {}", e.getMessage());
            throw e;
        }

    }

    private static void deleteAllFilesInDirectory(Path directory) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                Files.delete(file);
            }
            log.info("Deleted all files in directory: {}", directory);
        } catch (IOException e) {
            log.error("Failed to delete files in directory {}: {}", directory, e.getMessage());
        }
    }

    /**
     * Adds missing hourly rows to the processed data, with holiday column and mapTime adjustments.
     *
     * @param inputPath  The path to the current processed file.
     * @param outputPath The path to write the file with missing hourly rows added.
     * @throws IOException If there are errors during file processing.
     */
    private static void addMissingHourlyRows(Path inputPath, Path outputPath) throws IOException {
        log.info("Adding missing hourly rows...");

        var missingHourAdder = new MissingHourAdder(); // Create instance of the MissingHourAdder utility
        missingHourAdder.addMissingHours(inputPath, outputPath); // Execute the missing row addition
    }

}

/**
 * Helper class to manage file paths during different stages of processing.
 */
class ProcessedPaths {
    final String fileName;
    final Path validated;
    final Path invalid;
    final Path decimalShifted;
    final Path hourlyChecked;
    final Path atrOutput;
    final Path hourlyAtrOutput;    // For hourly data ATR
    final Path timeStampFormatted;
    final Path nameAppended;

    ProcessedPaths(String fileName, Path outputDirectory) {
        this.fileName = fileName;
        this.validated = outputDirectory.resolve("1_validated_" + fileName);
        this.invalid = outputDirectory.resolve("1_invalid_" + fileName);
        this.decimalShifted = outputDirectory.resolve("2_decimal_shifted_" + fileName);
        this.hourlyChecked = outputDirectory.resolve("3_checked_" + fileName);
        this.atrOutput = outputDirectory.resolve("4_atr_" + fileName);
        this.hourlyAtrOutput = outputDirectory.resolve("4_hourly_atr_" + fileName); // New path
        this.timeStampFormatted = outputDirectory.resolve("5_formatted_" + fileName);
        this.nameAppended = outputDirectory.resolve("6_with_name_" + fileName);
    }
}