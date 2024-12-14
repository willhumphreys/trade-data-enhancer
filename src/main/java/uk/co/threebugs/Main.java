package uk.co.threebugs;

import java.io.IOException;
import java.nio.file.Path;

public class Main {

    public static void main(String[] args) {
        var reader = new BitcoinMinuteDataReader();
        try {
            var data = reader.readFile(Path.of("data", "btcusd_1-min_data.csv"));
            data.forEach(System.out::println);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

}
