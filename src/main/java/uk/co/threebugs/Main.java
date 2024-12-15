package uk.co.threebugs;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Path;

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

            // Run the application
            var reader = new BitcoinMinuteDataReader();
            var path = Path.of("data", dataFile);

            try {
                var data = reader.readFile(path);
                new ATRAppender().appendATR(data, atrWindow).forEach(System.out::println);
            } catch (IOException e) {
                System.err.println("Error reading file: " + e.getMessage());
            }

        } catch (ParseException e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());

            // Print usage help
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Main", options);
        }
    }
}