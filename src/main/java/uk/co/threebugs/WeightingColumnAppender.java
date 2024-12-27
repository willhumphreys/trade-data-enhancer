package uk.co.threebugs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;

public class WeightingColumnAppender {

    public void addWeightingColumn(Path inputPath, Path outputPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(inputPath); BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Input file is empty!");
            }

            // Write new header with weighting and weightingAtr columns
            writer.write(header + ",weighting,weightingAtr");
            writer.newLine();

            TickWeigher weigher = new TickWeigher();
            String line;

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",");

                // Extract the Close price and ATR values
                BigDecimal closePrice = new BigDecimal(columns[5]); // Assuming Close is the 6th column (index 5)
                BigDecimal atr = new BigDecimal(columns[7]);       // Assuming ATR is the 8th column (index 7)

                // Calculate weightings
                String weighting = weigher.getWeighting(closePrice);
                String weightingAtr = weigher.getWeightingAtr(atr);

                // Append new columns to the row
                writer.write(line + "," + weighting + "," + weightingAtr);
                writer.newLine();
            }
        }
    }
}