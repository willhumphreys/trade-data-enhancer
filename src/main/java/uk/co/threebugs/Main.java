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


            var validatedPath = outputDirectory.resolve("validated_" + dataFile);
            var invalidPath = outputDirectory.resolve("invalid_" + dataFile);
            var hourlyCheckedPath = outputDirectory.resolve("checked_" + dataFile);

            log.info("Processing data from {}", inputPath);

            try {
                // Step 1: Validate the data
                var validator = new DataValidator();
                validator.validateDataFile(inputPath, validatedPath, invalidPath);

                log.info("data validated");


                // Step 2: Ensure hourly entries (streaming processing)
                var hourlyChecker = new HourlyDataChecker();
                hourlyChecker.ensureHourlyEntries(validatedPath, hourlyCheckedPath);

                log.info("hour entries checked");

                // Step 3: Append ATR values
                var reader = new BitcoinMinuteDataReader();
                var checkedData = reader.readFile(hourlyCheckedPath);
                var data = new ATRAppender().appendATR(checkedData, atrWindow).toList();

                log.info("Processed {} entries", data.size());

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