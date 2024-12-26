package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class Main {
    public static void main(String[] args) {
        // Create Apache Commons CLI options
        var options = new Options();
        options.addOption("w", "window", true, "ATR window size (e.g., 14 periods)");
        options.addOption("f", "file", true, "File name for the data (e.g., btcusd_1-min_data.csv)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            // Parse ATR window size (-w or --window, defaults to 14)
            var atrWindow = Integer.parseInt(cmd.getOptionValue("w", "14")); // Default to 14
            // Parse data file name (-f or --file, defaults to btcusd_1-min_data.csv)
            var dataFile = cmd.getOptionValue("f", "btcusd_1-min_data.csv");

            var dataDirectory = Path.of("data");
            var inputDirectory = dataDirectory.resolve("input");
            var outputDirectory = dataDirectory.resolve("output");

            deleteAllFilesInDirectory(outputDirectory);

            var inputPath = inputDirectory.resolve(dataFile);


            var validatedPath = outputDirectory.resolve("1_validated_" + dataFile);
            var invalidPath = outputDirectory.resolve("1_invalid_" + dataFile);
            var decimalShiftedPath = outputDirectory.resolve("2_decimal_shifted_" + dataFile);
            var hourlyCheckedPath = outputDirectory.resolve("3_checked_" + dataFile);
            var atrOutputPath = outputDirectory.resolve("4_atr_" + dataFile);
            var timeStampOutputPath = outputDirectory.resolve("5_formatted_" + dataFile);
            var nameAppendedPath = outputDirectory.resolve("6_with_name_" + dataFile);

            log.info("Processing data from {}", inputPath);

            try {
                // Step 1: Validate the data
                var validator = new DataValidator();
                validator.validateDataFile(inputPath, validatedPath, invalidPath);

                log.info("Data validated.");

                // Step 2: Shift decimals to integers
                var decimalShifter = new DecimalShifter();
                decimalShifter.shiftDecimalPlaces(validatedPath, decimalShiftedPath);

                log.info("Decimal values shifted to integers.");

                // Step 3: Ensure hourly entries (streaming processing)
                var hourlyChecker = new HourlyDataChecker();
                hourlyChecker.ensureHourlyEntries(decimalShiftedPath, hourlyCheckedPath);

                log.info("Hourly entries checked.");

                // Step 4: Append ATR values
                var reader = new BitcoinMinuteDataReader();
                var checkedData = reader.readFile(hourlyCheckedPath);
                new ATRAppender().appendATR(checkedData, atrWindow, atrOutputPath);

                // Step 5: Fix date formatting
                var timestampFormatter = new TimestampFormatter();
                timestampFormatter.reformatTimestamps(atrOutputPath, timeStampOutputPath);

                // Step 6: Add file name as a column
                var fileNameAppender = new FileNameAppender();
                String fileNameWithoutExtension = dataFile.substring(0, dataFile.lastIndexOf('.'));
                fileNameAppender.addFileNameColumn(timeStampOutputPath, nameAppendedPath, fileNameWithoutExtension);

                log.info("File name column added. Final output written to {}", nameAppendedPath);

                // Step 7: Add weighting column
                var weightingAdder = new WeightingColumnAppender();
                var finalOutputWithWeightingPath = outputDirectory.resolve("7_weighted_" + dataFile);

                weightingAdder.addWeightingColumn(nameAppendedPath, finalOutputWithWeightingPath);

                log.info("Weighting column added. Final output written to {}", finalOutputWithWeightingPath);

            } catch (IOException e) {
                log.error("Error during processing: {}", e.getMessage());
            }

        } catch (ParseException e) {
            log.error("Error parsing command line arguments: {}", e.getMessage());

            // Print usage help
            var formatter = new HelpFormatter();
            formatter.printHelp("Main", options);
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
}