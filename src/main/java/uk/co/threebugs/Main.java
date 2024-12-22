package uk.co.threebugs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class Main {
    public static void main(String[] args) {
        // Create Apache Commons CLI options
        Options options = new Options();
        options.addOption("w", "window", true, "ATR window size (e.g., 14 periods)");
        options.addOption("f", "file", true, "File name for the data (e.g., btcusd_1-min_data.csv)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            // Parse ATR window size (-w or --window, defaults to 14)
            int atrWindow = Integer.parseInt(cmd.getOptionValue("w", "14")); // Default to 14
            // Parse data file name (-f or --file, defaults to btcusd_1-min_data.csv)
            String dataFile = cmd.getOptionValue("f", "btcusd_1-min_data.csv");

            // Define paths for intermediate files
            Path inputPath = Path.of("data", dataFile);
            Path validatedPath = Path.of("data", "validated_" + dataFile);
            Path invalidPath = Path.of("data", "invalid_" + dataFile);
            Path hourlyCheckedPath = Path.of("data", "checked_" + dataFile);

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
                List<MinuteDataWithATR> data = new ATRAppender().appendATR(checkedData, atrWindow).toList();

                log.info("Processed {} entries", data.size());

            } catch (IOException e) {
                log.error("Error during processing: {}", e.getMessage());

            }

        } catch (ParseException e) {
            log.error("Error parsing command line arguments: {}", e.getMessage());

            // Print usage help
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Main", options);
        }
    }
}