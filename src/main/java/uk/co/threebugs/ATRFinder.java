package uk.co.threebugs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ATRFinder {

    public static void main(String[] args) {
        // Specify the path to the CSV file
        String filePath = "/home/will/code/mochi-java/data/spx-1m-btmF.csv"; // Update with the actual path to your file

        // Read the file to find the first row where ATR isn't -1
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            // Read the header
            String header = br.readLine();

            // Column indices
            int atrIndex = -1;
            int highIndex = -1;
            int lowIndex = -1;
            int weightingIndex = -1;
            int weightingATRIndex = -1;

            if (header != null) {
                String[] columns = header.split(",");
                for (int i = 0; i < columns.length; i++) {
                    if (columns[i].equalsIgnoreCase("atr")) {
                        atrIndex = i;
                    } else if (columns[i].equalsIgnoreCase("high")) {
                        highIndex = i;
                    } else if (columns[i].equalsIgnoreCase("low")) {
                        lowIndex = i;
                    } else if (columns[i].equalsIgnoreCase("weighting")) {
                        weightingIndex = i;
                    } else if (columns[i].equalsIgnoreCase("weightingAtr")) {
                        weightingATRIndex = i;
                    }
                }
            }

            // Validate if all necessary columns are found
            if (atrIndex == -1 || highIndex == -1 || lowIndex == -1 || weightingIndex == -1 || weightingATRIndex == -1) {
                System.out.println("One or more required columns (atr, high, low, weighting, weightingATR) were not found in the file.");
                return;
            }

            // Process each line
            String line;
            int rowNumber = 1; // Row counter for reporting
            while ((line = br.readLine()) != null) {
                rowNumber++;
                // Split the CSV row into columns
                String[] values = line.split(",");

                // Ensure the row has enough columns for the indices
                if (values.length <= Math.max(atrIndex, Math.max(highIndex, Math.max(lowIndex, Math.max(weightingIndex, weightingATRIndex))))) {
                    continue;
                }

                // Parse the ATR value
                try {
                    double atrValue = Double.parseDouble(values[atrIndex]);
                    if (atrValue != -1) {
                        // Parse high and low
                        double high = Double.parseDouble(values[highIndex]);
                        double low = Double.parseDouble(values[lowIndex]);
                        double range = high - low;

                        // Parse weighting and weightingATR
                        double weighting = Double.parseDouble(values[weightingIndex]);
                        double weightingATR = Double.parseDouble(values[weightingATRIndex]);

                        // Print the results
                        System.out.println("First row where ATR isn't -1 found at row " + rowNumber + ":");
                        System.out.println("Low: " + low);
                        System.out.println("High: " + high);
                        System.out.println("Range: " + range);
                        System.out.println("ATR: " + atrValue);
                        System.out.println("Weighting: " + weighting);
                        System.out.println("Weighting ATR: " + weightingATR);

                        return;
                    }
                } catch (NumberFormatException e) {
                    // If parsing fails for a numeric field, skip the row
                }
            }

            // If no row is found with ATR not equal to -1
            System.out.println("No row with ATR not equal to -1 found in the file.");
        } catch (IOException e) {
            System.out.println("An error occurred while reading the file: " + e.getMessage());
        }
    }
}