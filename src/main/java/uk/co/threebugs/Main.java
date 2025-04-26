package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
            processCommandLine(args);
    }

    /**
     * Process command line arguments and execute the application logic
     *
     * @param args Command-line arguments
     * @throws Exception if any error occurs during processing
     */
    private static void processCommandLine(String[] args) throws Exception {
        // Create command line options
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Error parsing command line arguments: {}", e.getMessage());
            formatter.printHelp("Main", options);
            throw new IllegalArgumentException("Failed to parse command line arguments", e);
        }

        // Check if we have any arguments at all
        if (args.length == 0) {
            formatter.printHelp("Main", options);
            throw new IllegalArgumentException("No command line arguments provided");
        }

        // Extract required options with validation
        String ticker = validateRequiredOption(cmd, "ticker", "ticker symbol");
        String provider = validateRequiredOption(cmd, "provider", "provider");
        String s3KeyMin = validateRequiredOption(cmd, "s3_key_min", "S3 key for minute data");
        String s3KeyHour = validateRequiredOption(cmd, "s3_key_hour", "S3 key for hourly data");
        String s3KeyDay = validateRequiredOption(cmd, "s3_key_day", "S3 key for daily data");


        // Extract optional options
        int atrWindow = Integer.parseInt(cmd.getOptionValue("w", "14")); // Default to 14 if not provided

        // Read environment variables for bucket names
        String inputBucketName = System.getenv("INPUT_BUCKET_NAME");
        String outputBucketName = System.getenv("OUTPUT_BUCKET_NAME");

        // Check if environment variables are available
        if (inputBucketName == null || inputBucketName.isEmpty()) {
            throw new IllegalStateException("INPUT_BUCKET_NAME environment variable is not set");
        }

        if (outputBucketName == null || outputBucketName.isEmpty()) {
            throw new IllegalStateException("OUTPUT_BUCKET_NAME environment variable is not set");
        }

        // Log the values for debugging
        log.info("Starting processing with parameters:");
        log.info("  Ticker: {}", ticker);
        log.info("  Provider: {}", provider);
        log.info("  S3 Key Min: {}", s3KeyMin);
        log.info("  S3 Key Hour: {}", s3KeyHour);
        log.info("  S3 Key Day: {}", s3KeyDay);
        log.info("  ATR Window: {}", atrWindow);
        log.info("  Input Bucket: {}", inputBucketName);
        log.info("  Output Bucket: {}", outputBucketName);


        // Continue with your application logic
        // This is where you'd call the data processing methods
        executeDataProcessing(ticker, provider, s3KeyMin, s3KeyHour, s3KeyDay, atrWindow, inputBucketName, outputBucketName);
    }

    /**
     * Execute the core data processing logic
     */
    private static void executeDataProcessing(String ticker, String provider, String s3KeyMin, String s3KeyHour, String s3KeyDay, int atrWindow, String inputBucketName, String outputBucketName) throws IOException {

        // Initialize S3 client
        S3Client s3Client = S3Client.builder().region(Region.EU_CENTRAL_1) // Adjust region as needed
                .build();

        // Data directories
        Path dataDir = Paths.get(System.getProperty("user.dir"), "data");
        Files.createDirectories(dataDir);


        // Initialize DataFetcher
        DataFetcher dataFetcher = new DataFetcher(ticker, provider, dataDir, s3Client);

        // Fetch data from S3 using the input bucket name and S3 keys
        Map<String, DataFetcher.DataFileInfo> dataFiles = dataFetcher.fetchData(inputBucketName, s3KeyMin, s3KeyHour, s3KeyDay);


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
        DataFetcher.DataFileInfo minuteDataPath;
        DataFetcher.DataFileInfo hourlyDataPath;
        DataFetcher.DataFileInfo dailyDataPath;

        log.info("Data file not specified or not found. Attempting to fetch from S3...");

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

        // Extract filenames from paths
        String minuteDataFile = minuteDataPath.getLocalPath().getFileName().toString();
        String hourlyDataFile = hourlyDataPath.getLocalPath().getFileName().toString();
        String dailyDataFile = dailyDataPath.getLocalPath().getFileName().toString();

        // Instance of the trimmer
        MinuteDataTrimmer trimmer = new MinuteDataTrimmer();

        // Output paths for intermediate processing
        var processedMinutePaths = new ProcessedPaths(minuteDataFile, outputDirectory);
        var processedHourlyPaths = new ProcessedPaths(hourlyDataFile, outputDirectory);
        var processedDailyPaths = new ProcessedPaths(dailyDataFile, outputDirectory);

        // Trim the minute data
        trimmer.trimMinuteData(minuteDataPath.getLocalPath(), hourlyDataPath.getLocalPath(), processedMinutePaths.trimmedMinuteOutput);

        log.info("Minute data successfully trimmed and written to {}.", processedMinutePaths.trimmedMinuteOutput);

        log.info("Starting data processing for Minute Data: {} and Hourly Data: {}", processedMinutePaths.trimmedMinuteOutput, hourlyDataFile);

        // Process minute data (validation, decimal shift, and timestamp check)
        processMinuteData(processedMinutePaths.trimmedMinuteOutput, processedMinutePaths);

        // Process hourly data (validation, decimal shift, and timestamp check)
        processData(hourlyDataPath.getLocalPath(), atrWindow, processedHourlyPaths.validated, processedHourlyPaths.invalid, processedHourlyPaths.decimalShifted, processedHourlyPaths.sorted, processedHourlyPaths.deduplicated, processedHourlyPaths.timeFrameAtrOutput, "Hourly");

        // Process daily data
        processData(dailyDataPath.getLocalPath(), 14, processedDailyPaths.validated, processedDailyPaths.invalid, processedDailyPaths.decimalShifted, processedDailyPaths.sorted, processedDailyPaths.deduplicated, processedDailyPaths.timeFrameAtrOutput, "Daily");

        // Combine hourly and minute data for integrity check
        performDataIntegrityCheck(processedMinutePaths.decimalShifted, processedMinutePaths.hourlyChecked, processedDailyPaths.timeFrameAtrOutput, processedMinutePaths.atrOutput);

        // Additional processing for ATR and formatting
        Path inputPath = performAdditionalProcessing(processedMinutePaths);

        String s3Key = new DataUploader(s3Client).uploadMinuteData(inputPath, outputBucketName, minuteDataPath.getS3Path());

        log.info("Data successfully uploaded to S3. S3 key: {}", s3Key);


    }

    /**
     * Validates that a required option is present and has a value
     *
     * @param cmd         Command line
     * @param opt         Option name
     * @param description Description for error reporting
     * @return The option value
     * @throws IllegalArgumentException if the option is missing
     */
    private static String validateRequiredOption(CommandLine cmd, String opt, String description) {
        if (!cmd.hasOption(opt)) {
            String errorMsg = "Missing required option: " + opt + " (" + description + ")";
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        return cmd.getOptionValue(opt);
    }


    private static Options createOptions() {
        Options options = new Options();

        options.addOption(Option.builder("t").longOpt("ticker").hasArg().desc("Symbol (e.g., AAPL)").required(true).build());

        options.addOption(Option.builder("p").longOpt("provider").hasArg().desc("Data provider (e.g., polygon)").required(true).build());

        options.addOption(Option.builder("m").longOpt("s3_key_min").hasArg().desc("S3 key for minute data").required(true).build());

        options.addOption(Option.builder("h").longOpt("s3_key_hour").hasArg().desc("S3 key for hourly data").required(true).build());

        options.addOption(Option.builder("d").longOpt("s3_key_day").hasArg().desc("S3 key for daily data").required(true).build());

        options.addOption(Option.builder("w").longOpt("window").hasArg().desc("ATR window size (default: 14)").required(false).build());

        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("Input file name (optional)").required(false).build());

        return options;
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
        log.info("Appending ATR values to " + timeFrame + " data...");
//      var atrAppender = new ATRAppender();


        var atrScalingFactorAppender = new ATRScalingFactorAppender();
        var longReader = new BitcoinLongDataReader();

        var hourlyDataLong = longReader.readFile(deduplicated); // Read from deduplicated file
        int shortPeriod = 20;
        int longPeriod = 42;
        BigDecimal alpha = new BigDecimal("0.5");
        atrScalingFactorAppender.writeStreamToFile(atrScalingFactorAppender.appendScalingFactor(hourlyDataLong, shortPeriod, longPeriod, alpha), hourlyAtrOutput);
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
