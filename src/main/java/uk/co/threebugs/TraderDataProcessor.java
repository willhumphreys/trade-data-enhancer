package uk.co.threebugs;

import java.io.*;

public class TraderDataProcessor {

    public static void main(String[] args) {
        // Specify the input file and output file paths
        String inputFilePath = "/home/will/IdeaProjects/ConsectiveWinnersLosers/output6/spx-1m-btmF/spx-1m-btmF-filtered-long.csv"; // Replace with the actual input file path
        String outputFilePath = "/home/will/IdeaProjects/ConsectiveWinnersLosers/output6/spx-1m-btmF/spx-1m-btmF-filtered-long-atr.csv"; // Replace with the desired output file path
        double multiplier = 3.91; // Fixed multiplier for stop, limit, and tickoffset

        // Process the file
        try {
            processFile(inputFilePath, outputFilePath, multiplier);
            System.out.println("File processed successfully. Output written to " + outputFilePath);
        } catch (IOException e) {
            System.err.println("An error occurred while processing the file: " + e.getMessage());
        }
    }

    public static void processFile(String inputFilePath, String outputFilePath, double multiplier) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {

            String headerLine = br.readLine(); // Read the header
            if (headerLine == null) {
                throw new IOException("Input file is empty.");
            }

            bw.write(headerLine); // Write the header to the output file
            bw.newLine();

            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");

                // Validate column count
                if (columns.length < 7) {
                    System.err.println("Skipping malformed line: " + line);
                    continue;
                }

                try {
                    // Parse and multiply relevant columns
                    int stop = (int) (Integer.parseInt(columns[4]) * multiplier);
                    int limit = (int) (Integer.parseInt(columns[5]) * multiplier);
                    int tickoffset = (int) (Integer.parseInt(columns[6]) * multiplier);

                    // Replace the original columns with the updated values
                    columns[4] = String.valueOf(stop);
                    columns[5] = String.valueOf(limit);
                    columns[6] = String.valueOf(tickoffset);

                    // Write the updated line to the output file
                    bw.write(String.join(",", columns));
                    bw.newLine();
                } catch (NumberFormatException e) {
                    System.err.println("Skipping line with invalid numeric values: " + line);
                }
            }
        }
    }
}