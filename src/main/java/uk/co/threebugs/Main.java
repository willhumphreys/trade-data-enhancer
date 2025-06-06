package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import static uk.co.threebugs.TimeFrame.DAILY;
import static uk.co.threebugs.TimeFrame.HOURLY;

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
        int shortAtrPeriod = Integer.parseInt(validateRequiredOption(cmd, "short_atr_period", "short ATR period"));
        int longAtrPeriod = Integer.parseInt(validateRequiredOption(cmd, "long_atr_period", "long ATR period"));
        double alpha = Double.parseDouble(validateRequiredOption(cmd, "alpha", "alpha value"));
        int atrWindow = Integer.parseInt(cmd.getOptionValue("w", "14")); // Default to 14 if not provided
        String backTestId = validateRequiredOption(cmd, "back_test_id", "Back test ID");

        // Read environment variables for bucket names
        String inputBucketName = System.getenv("INPUT_BUCKET_NAME");
        String outputBucketName = System.getenv("OUTPUT_BUCKET_NAME");
        String mochiProdBacktestParamsBucket = System.getenv("MOCHI_PROD_BACKTEST_PARAMS");

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
        log.info("  Short ATR Period: {}", shortAtrPeriod);
        log.info("  Long ATR Period: {}", longAtrPeriod);
        log.info("  Alpha: {}", alpha);
        log.info(" BackTestId: {}", backTestId);

        // Continue with your application logic
        // This is where you'd call the data processing methods
        executeDataProcessing(ticker, provider, s3KeyMin, s3KeyHour, s3KeyDay, inputBucketName, outputBucketName, shortAtrPeriod, longAtrPeriod, alpha, backTestId, mochiProdBacktestParamsBucket);
    }

    /**
     * Execute the core data processing logic
     */
    private static void executeDataProcessing(String ticker, String provider, String s3KeyMin, String s3KeyHour, String s3KeyDay, String inputBucketName, String outputBucketName, int shortATRPeriod, int longATRPeriod, double alpha, String backTestId, String mochiProdBacktestParamsBucket) throws IOException {


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


        // Output paths for intermediate processing
        var processedMinutePaths = new ProcessedPaths(minuteDataFile, outputDirectory);
        var processedHourlyPaths = new ProcessedPaths(hourlyDataFile, outputDirectory);
        var processedDailyPaths = new ProcessedPaths(dailyDataFile, outputDirectory);


        log.info("Starting data processing for Minute Data: {} and Hourly Data: {}", minuteDataPath.getLocalPath(), hourlyDataPath.getLocalPath());

        // Process minute data (validation, decimal shift, and timestamp check)
        processMinuteData(minuteDataPath.getLocalPath(), processedMinutePaths);

        // Process hourly data (validation, decimal shift, and timestamp check)
        processData(hourlyDataPath.getLocalPath(), processedHourlyPaths.validated, processedHourlyPaths.invalid, processedHourlyPaths.decimalShifted, processedHourlyPaths.sorted, processedHourlyPaths.deduplicated, processedHourlyPaths.timeFrameAtrOutput, HOURLY, shortATRPeriod, longATRPeriod, alpha);

        // Process daily data
        processData(dailyDataPath.getLocalPath(), processedDailyPaths.validated, processedDailyPaths.invalid, processedDailyPaths.decimalShifted, processedDailyPaths.sorted, processedDailyPaths.deduplicated, processedDailyPaths.timeFrameAtrOutput, DAILY, shortATRPeriod, longATRPeriod, alpha);

        // Combine hourly and minute data for integrity check
        performDataIntegrityCheck(processedMinutePaths.decimalShifted, processedMinutePaths.hourlyChecked, processedDailyPaths.timeFrameAtrOutput, processedMinutePaths.atrOutput);

        // Additional processing for ATR and formatting
        Path inputPath = performAdditionalProcessing(processedMinutePaths);

        // Load minute data and print header and last row
        String weightingAtr = printMinuteDataHeaderAndLastRow(inputPath);

        updateJsonWithWeightingAtr(s3Client, backTestId, mochiProdBacktestParamsBucket, weightingAtr);


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

        options.addOption(Option.builder("w").longOpt("window").hasArg().desc("ATR window size").required(false).build());

        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("Input file name").required(false).build());

        options.addOption(Option.builder().longOpt("short_atr_period").hasArg().desc("Short ATR period").required(true).build());

        options.addOption(Option.builder().longOpt("long_atr_period").hasArg().desc("Long ATR period").required(true).build());

        options.addOption(Option.builder().longOpt("alpha").hasArg().desc("Alpha value").required(true).build());

        options.addOption(Option.builder().longOpt("back_test_id").hasArg().desc("Back test ID").required(true).build());

        return options;
    }


    private static void processData(Path dataPath, Path validated, Path invalidPath, Path decimalShifted, Path sorted, Path deduplicated, Path hourlyAtrOutput, TimeFrame timeFrame, int shortATRPeriod, int longATRPeriod, double alpha) throws IOException {
        log.info("Processing " + timeFrame + " data...");

        var validator = new DataValidator();
        var decimalShifter = new DecimalShifter();
        var fileSorter = new FileSorter();
        var duplicateRemover = new DuplicateRemover();
        var timestampOrderChecker = new TimestampOrderChecker();

        // Step 1: Validate hourly data
        validator.validateDataFile(dataPath, validated, invalidPath);
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
        log.info("Appending ATR values to {} data...", timeFrame);
        var atrAppender = new HybridScalingAppender(shortATRPeriod, longATRPeriod, alpha, timeFrame);
        var longReader = new BitcoinLongDataReader();

        var hourlyDataLong = longReader.readFile(deduplicated); // Read from deduplicated file
        atrAppender.writeStreamToFile(atrAppender.appendScalingFactors(hourlyDataLong), hourlyAtrOutput);
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
     * @param dailyDataWithHybridRatio
     * @param atrOutput
     * @throws IOException
     */
    private static void performDataIntegrityCheck(Path minuteData, Path hourlyCheckedMinuteData, Path dailyDataWithHybridRatio, Path atrOutput) throws IOException {
        log.info("Performing data integrity check...");

        var hourlyChecker = new HourlyDataChecker();

        // Step 1: Ensure hourly entries exist in minute data
        int hourlyEntries = hourlyChecker.ensureHourlyEntries(minuteData, dailyDataWithHybridRatio, hourlyCheckedMinuteData);
        log.info("Inserted hours: {}", hourlyEntries);

        // Step 2: Validate data integrity
        var integrityChecker = new DataIntegrityChecker();
        String integrityResult = integrityChecker.validateDataIntegrity(hourlyCheckedMinuteData, dailyDataWithHybridRatio);

        if (!Objects.equals(integrityResult, "No issues found.")) {
            log.error("Data integrity check failed! {}", integrityResult);
            throw new IllegalStateException("Data integrity check failed! " + integrityResult);
        }
        log.info("Data integrity check passed.");

        // Step 3: Copy ATR values from hourly data to minute data
        //CopyHourlyATRToMinute.copyHourlyATRToMinute(hourlyCheckedMinuteData, dailyDataWithHybridRatio, atrOutput);
        CopyDailyATRToMinute.copyDailyATRToMinute(hourlyCheckedMinuteData, dailyDataWithHybridRatio, atrOutput);
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

    /**
     * Loads minute data from the specified file and prints the header and last row.
     */
    /**
     * Updates a JSON file in S3 with the weightingAtr value.
     *
     * @param s3Client     The S3 client to use for S3 operations
     * @param backTestId   The back test ID to use as the key for the JSON file
     * @param bucketName   The name of the S3 bucket where the JSON file is stored
     * @param weightingAtr The weightingAtr value to add to the JSON file
     * @throws IOException If there are errors during file operations
     */
    private static void updateJsonWithWeightingAtr(S3Client s3Client, String backTestId, String bucketName, String weightingAtr) throws IOException {
        if (weightingAtr == null) {
            log.warn("weightingAtr value is null. Skipping JSON update.");
            throw new IllegalArgumentException("weightingAtr value is null. Skipping JSON update.");
        }

        log.info("Updating JSON file in S3 with weightingAtr value: {}", weightingAtr);

        // Create the S3 key for the JSON file
        String jsonKey = backTestId + ".json";
        log.info("JSON file key: {}", jsonKey);

        // Create a temporary directory for downloading and uploading the JSON file
        Path tempDir = Files.createTempDirectory("json_update");
        Path jsonFilePath = tempDir.resolve("params.json");

        try {
            // Download the JSON file from S3
            log.info("Downloading JSON file from S3: s3://{}/{}", bucketName, jsonKey);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(jsonKey).build();

            s3Client.getObject(getObjectRequest, software.amazon.awssdk.core.sync.ResponseTransformer.toFile(jsonFilePath.toFile()));

            // Read the JSON file
            String jsonContent = Files.readString(jsonFilePath);
            log.info("Original JSON content: {}", jsonContent);

            // Update the JSON content with the weightingAtr value
            String updatedJsonContent;
            if (jsonContent.trim().endsWith("}")) {
                // Add the weightingAtr field before the closing brace
                updatedJsonContent = jsonContent.substring(0, jsonContent.lastIndexOf("}")).trim();
                if (updatedJsonContent.endsWith(",")) {
                    updatedJsonContent += " \"weightingAtr\": " + weightingAtr + " }";
                } else {
                    updatedJsonContent += ", \"weightingAtr\": " + weightingAtr + " }";
                }
            } else {
                log.error("Invalid JSON format. Unable to update JSON file.");
                throw new IOException("Invalid JSON format. Unable to update JSON file.");
            }

            log.info("Updated JSON content: {}", updatedJsonContent);

            // Write the updated JSON content to the file
            Files.writeString(jsonFilePath, updatedJsonContent);

            // Upload the updated JSON file to S3
            log.info("Uploading updated JSON file to S3: s3://{}/{}", bucketName, jsonKey);
            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(jsonKey).build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(jsonFilePath));

            log.info("Successfully updated JSON file in S3 with weightingAtr value: {}", weightingAtr);
        } catch (Exception e) {
            log.error("Error updating JSON file in S3: {}", e.getMessage(), e);
            throw new IOException("Error updating JSON file in S3", e);
        } finally {
            // Clean up temporary files
            Files.deleteIfExists(jsonFilePath);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Loads minute data from the specified file and prints the header and last row.
     *
     * @param filePath Path to the minute data file
     * @return The weightingAtr value from the last row
     * @throws IOException If there are errors during file reading
     */
    private static String printMinuteDataHeaderAndLastRow(Path filePath) throws IOException {
        log.info("Loading minute data to print header and last row...");

        String header = null;
        String lastLine = null;
        String weightingAtr = null;

        // Read the file line by line to get both header and last line
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
            String line;
            header = reader.readLine(); // First line is the header

            // Continue reading until the end to get the last line
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }
        }

        // Print header for reference
        log.info("Minute data header: {}", header);

        if (lastLine != null) {
            // Print last row for reference
            log.info("Minute data last row: {}", lastLine);

            // Format and print aligned header and values
            String[] headerFields = header.split(",");
            String[] valueFields = lastLine.split(",");

            StringBuilder alignedOutput = new StringBuilder("Aligned minute data (header=value):\n");

            // Determine the maximum length of header fields for alignment
            int maxHeaderLength = 0;
            for (String field : headerFields) {
                maxHeaderLength = Math.max(maxHeaderLength, field.length());
            }

            // Format string with padding based on the maximum header length
            String formatString = "  %-" + (maxHeaderLength + 2) + "s %s\n";

            // Build the aligned output and extract weightingAtr value
            int minLength = Math.min(headerFields.length, valueFields.length);
            for (int i = 0; i < minLength; i++) {
                alignedOutput.append(String.format(formatString, headerFields[i] + ":", valueFields[i]));

                // Extract weightingAtr value
                if (headerFields[i].equals("weightingAtr")) {
                    weightingAtr = valueFields[i];
                    log.info("Extracted weightingAtr value: {}", weightingAtr);
                }
            }

            log.info(alignedOutput.toString());
        } else {
            log.warn("No data found in the minute data file.");
        }

        return weightingAtr;
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
    }
}
