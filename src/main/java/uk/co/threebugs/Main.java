package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class Main {


    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption("w", "window", true, "ATR window size");
        options.addOption("f", "file", true, "Input file name");
        options.addOption("t", "ticker", true, "Symbol (e.g., AAPL)");
        options.addOption("p", "provider", true, "Data provider (e.g., polygon)");
        options.addOption("h", "help", false, "Show help");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        S3Client s3Client = S3Client.builder().region(Region.EU_CENTRAL_1).build();

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                formatter.printHelp("Main", options);
                System.exit(0);
            }

            int atrWindow = Integer.parseInt(cmd.getOptionValue("w", "14"));
            String fileName = cmd.getOptionValue("f");
            options.addOption("t", "ticker", true, "Ticker symbol to process");
            options.addOption("p", "provider", true, "Data provider");


            String inputBucket = System.getenv("INPUT_BUCKET_NAME");
            String outputBucket = System.getenv("OUTPUT_BUCKET_NAME");


            String ticker;
            if (cmd.hasOption("t")) {
                ticker = cmd.getOptionValue("t");
            } else {
                throw new IllegalArgumentException("Ticker symbol must be specified.");
            }

            String provider;
            if (cmd.hasOption("p")) {
                provider = cmd.getOptionValue("p");
            } else {
                throw new IllegalArgumentException("Data provider must be specified.");
            }

            if (inputBucket == null) {
                log.error("Input bucket name not specified. Set an environmental variable with the value INPUT_BUCKET_NAME. Aborting.");
                System.exit(1);
            }

            if (outputBucket == null) {
                log.error("Input bucket name not specified. Set an environmental variable with the value OUTPUT_BUCKET_NAME Aborting.");
                System.exit(1);
            }

            // Data directories
            Path dataDir = Paths.get("data");
            Files.createDirectories(dataDir);
            Path minuteDir = dataDir.resolve("minute");
            Path hourlyDir = dataDir.resolve("hourly");
            Path dailyDir = dataDir.resolve("daily");
            Files.createDirectories(minuteDir);
            Files.createDirectories(hourlyDir);
            Files.createDirectories(dailyDir);

            // Output directory
            Path outputDirectory = Paths.get("output");
            Files.createDirectories(outputDirectory);

            // If no file was specified or the file doesn't exist, fetch from S3
            Path minuteDataPath;
            Path hourlyDataPath;
            Path dailyDataPath;

            if (fileName == null || !Files.exists(minuteDir.resolve(fileName))) {
                log.info("Data file not specified or not found. Attempting to fetch from S3...");

                DataFetcher dataFetcher = new DataFetcher(ticker, provider, dataDir, s3Client);
                Map<String, Path> dataFiles = dataFetcher.fetchData(inputBucket);

                if (dataFiles.isEmpty()) {
                    log.error("Failed to fetch required data files. Aborting.");
                    System.exit(1);
                }

                minuteDataPath = dataFiles.get("1min");
                hourlyDataPath = dataFiles.get("1hour");
                dailyDataPath = dataFiles.get("1day");

                log.info("Successfully fetched data files:");
                log.info("  Minute data: {}", minuteDataPath);
                log.info("  Hourly data: {}", hourlyDataPath);
                log.info("  Daily data: {}", dailyDataPath);
            } else {
                minuteDataPath = minuteDir.resolve(fileName);
                // Use default naming pattern for related files when only minute file is specified
                String baseName = fileName.replaceAll("_1-min_", "_");
                hourlyDataPath = hourlyDir.resolve(baseName.replaceAll("\\.csv$", "_1-hour.csv"));
                dailyDataPath = dailyDir.resolve(baseName.replaceAll("\\.csv$", "_1-day.csv"));

                log.info("Using specified data files:");
                log.info("  Minute data: {}", minuteDataPath);
                log.info("  Hourly data: {}", hourlyDataPath);
                log.info("  Daily data: {}", dailyDataPath);
            }

            // Extract filenames from paths
            String minuteDataFile = minuteDataPath.getFileName().toString();
            String hourlyDataFile = hourlyDataPath.getFileName().toString();
            String dailyDataFile = dailyDataPath.getFileName().toString();

            // Instance of the trimmer
            MinuteDataTrimmer trimmer = new MinuteDataTrimmer();

            // Output paths for intermediate processing
            var processedMinutePaths = new ProcessedPaths(minuteDataFile, outputDirectory);
            var processedHourlyPaths = new ProcessedPaths(hourlyDataFile, outputDirectory);
            var processedDailyPaths = new ProcessedPaths(dailyDataFile, outputDirectory);

            // Trim the minute data
            trimmer.trimMinuteData(minuteDataPath, hourlyDataPath, processedMinutePaths.trimmedMinuteOutput);

            log.info("Minute data successfully trimmed and written to {}.", processedMinutePaths.trimmedMinuteOutput);

            log.info("Starting data processing for Minute Data: {} and Hourly Data: {}",
                    processedMinutePaths.trimmedMinuteOutput, hourlyDataFile);

            // Process minute data (validation, decimal shift, and timestamp check)
            processMinuteData(processedMinutePaths.trimmedMinuteOutput, processedMinutePaths);

            // Process hourly data (validation, decimal shift, and timestamp check)
            processData(hourlyDataPath, atrWindow,
                    processedHourlyPaths.validated,
                    processedHourlyPaths.invalid,
                    processedHourlyPaths.decimalShifted,
                    processedHourlyPaths.sorted,
                    processedHourlyPaths.deduplicated,
                    processedHourlyPaths.timeFrameAtrOutput,
                    "Hourly");

            // Process daily data
            processData(dailyDataPath, 14,
                    processedDailyPaths.validated,
                    processedDailyPaths.invalid,
                    processedDailyPaths.decimalShifted,
                    processedDailyPaths.sorted,
                    processedDailyPaths.deduplicated,
                    processedDailyPaths.timeFrameAtrOutput,
                    "Daily");

            // Combine hourly and minute data for integrity check
            performDataIntegrityCheck(
                    processedMinutePaths.decimalShifted,
                    processedMinutePaths.hourlyChecked,
                    processedDailyPaths.timeFrameAtrOutput,
                    processedMinutePaths.atrOutput);

            // Additional processing for ATR and formatting
            Path inputPath = performAdditionalProcessing(processedMinutePaths);


            String s3Key = new DataUploader(ticker, provider, s3Client).uploadMinuteData(inputPath, outputBucket);

            log.info("Data successfully uploaded to S3. S3 key: {}", s3Key);

        } catch (ParseException e) {
            log.error("Error parsing command line arguments", e);
            formatter.printHelp("Main", options);
            System.exit(1);
        }
    }



    private static void processData(Path hourlyDataPath, int atrWindow, Path validated, Path invalidPath, Path decimalShifted, Path sorted, Path deduplicated, Path hourlyAtrOutput, String timeFrame) throws IOException {
        log.info("Processing " + timeFrame + " data...");

        var validator = new DataValidator();
        var decimalShifter = new DecimalShifter();
        var fileSorter = new FileSorter();
        var duplicateRemover = new DuplicateRemover();
        var timestampOrderChecker = new TimestampOrderChecker();

        // Step 1: Validate hourly data
        validator.validateDataFile(hourlyDataPath, validated, invalidPath);
        log.info(timeFrame + " data validated.");

        // Step 2: Decimal shift
        Path shiftValuePath = validated.getParent().resolve("decimal_shift_value.txt");
        if (!Files.exists(shiftValuePath)) {
            throw new IOException("Decimal shift value file not found. Ensure minute data is processed first.");
        }

        int decimalShift = Integer.parseInt(Files.readString(shiftValuePath).trim());
        decimalShifter.shiftDecimalPlacesWithPredefinedShift(validated, decimalShifted, decimalShift);
        log.info(timeFrame + " data decimal values adjusted using predefined shift value: {}.", decimalShift);

        // Step 3: Sort the file
        fileSorter.sortFileByTimestamp(decimalShifted, sorted);
        log.info(timeFrame + " data sorted by timestamp.");

        // Step 4: Remove duplicate timestamps
        duplicateRemover.removeDuplicates(sorted, deduplicated);
        log.info("Duplicates removed and deduplicated file written to: {}", deduplicated);

        // Step 5: Verify timestamps
        timestampOrderChecker.checkTimestampOrder(deduplicated);
        log.info(timeFrame + " timestamps verified to be correct.");

        // Step 6: Append ATR values
        log.info("Appending ATR values to hourly data...");
        var atrAppender = new ATRAppender();
        var longReader = new BitcoinLongDataReader();

        var hourlyDataLong = longReader.readFile(deduplicated); // Read from deduplicated file
        atrAppender.writeStreamToFile(atrAppender.appendATR(hourlyDataLong, atrWindow), hourlyAtrOutput);
        log.info("ATR values successfully added to " + timeFrame + " data.");
    }

    private static void processMinuteData(Path minuteDataPath, ProcessedPaths paths) throws IOException {
        log.info("Processing minute data...");

        var validator = new DataValidator();
        var decimalShifter = new DecimalShifter();
        var fileSorter = new FileSorter();
        var duplicateRemover = new DuplicateRemover();
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

        // Step 3: Sort the file
        fileSorter.sortFileByTimestamp(paths.decimalShifted, paths.sorted);
        log.info("Minute data sorted by timestamp.");

        // Step 4: Remove duplicate timestamps
        duplicateRemover.removeDuplicates(paths.sorted, paths.deduplicated);
        log.info("Duplicates removed and deduplicated file written to: {}", paths.deduplicated);

        // Step 5: Verify timestamps are in order
        try {
            timestampOrderChecker.checkTimestampOrder(paths.deduplicated);
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
     * @param timeFrameAtrOutput
     * @param atrOutput
     * @throws IOException
     */
    private static void performDataIntegrityCheck(Path minuteData, Path hourlyCheckedMinuteData, Path timeFrameAtrOutput, Path atrOutput) throws IOException {
        log.info("Performing data integrity check...");

        var hourlyChecker = new HourlyDataChecker();

        // Step 1: Ensure hourly entries exist in minute data
        int hourlyEntries = hourlyChecker.ensureHourlyEntries(minuteData, timeFrameAtrOutput, hourlyCheckedMinuteData);
        log.info("Inserted hours: {}", hourlyEntries);

        // Step 2: Validate data integrity
        var integrityChecker = new DataIntegrityChecker();
        String integrityResult = integrityChecker.validateDataIntegrity(hourlyCheckedMinuteData, timeFrameAtrOutput);

        if (!Objects.equals(integrityResult, "No issues found.")) {
            log.error("Data integrity check failed! {}", integrityResult);
            throw new IllegalStateException("Data integrity check failed! " + integrityResult);
        }
        log.info("Data integrity check passed.");

        // Step 3: Copy ATR values from hourly data to minute data
        //CopyHourlyATRToMinute.copyHourlyATRToMinute(hourlyCheckedMinuteData, timeFrameAtrOutput, atrOutput);
        CopyDailyATRToMinute.copyDailyATRToMinute(hourlyCheckedMinuteData, timeFrameAtrOutput, atrOutput);
        log.info("Successfully copied ATR values from hourly data to minute data.");


    }

    private static Path performAdditionalProcessing(ProcessedPaths paths) throws IOException {
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
        var atrRatioOutputPath = paths.hourlyChecked.getParent().resolve("7_atr_ratio_" + paths.fileName);
        weightingAdder.addWeightingColumn(paths.nameAppended, weightedOutputPath, atrRatioOutputPath);
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

        return missingHourlyPath;

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
        long addedMissingHours = missingHourAdder.addMissingHours(inputPath, outputPath);// Execute the missing row addition

        log.info("Added holiday hours {}", addedMissingHours);
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
    final Path sorted;
    final Path deduplicated;      // New path for deduplicated file
    final Path hourlyChecked;
    final Path atrOutput;
    final Path timeFrameAtrOutput;
    final Path timeStampFormatted;
    final Path nameAppended;
    final Path trimmedMinuteOutput;

    ProcessedPaths(String fileName2, Path outputDirectory) {

        Path path = Path.of(fileName2);

        String fileName = path.getName(path.getNameCount() - 1).toString();

        this.fileName = fileName;
        this.validated = outputDirectory.resolve("1_validated_" + fileName);
        this.invalid = outputDirectory.resolve("1_invalid_" + fileName);
        this.decimalShifted = outputDirectory.resolve("2_decimal_shifted_" + fileName);
        this.sorted = outputDirectory.resolve("2_sorted_" + fileName);
        this.deduplicated = outputDirectory.resolve("3_deduplicated_" + fileName); // Add new output path
        this.hourlyChecked = outputDirectory.resolve("4_checked_" + fileName);
        this.atrOutput = outputDirectory.resolve("5_atr_" + fileName);
        this.timeFrameAtrOutput = outputDirectory.resolve("5_hourly_atr_" + fileName);
        this.timeStampFormatted = outputDirectory.resolve("6_formatted_" + fileName);
        this.nameAppended = outputDirectory.resolve("7_with_name_" + fileName);
        this.trimmedMinuteOutput = outputDirectory.resolve("1.1_trimmed_" + fileName);
    }
}
