package utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class EpochToLocalDateTime {
    public static void main(String[] args) {
        // Example epoch second value
//        long epochSecond = 1735225200L;
        long epochSecond = 1694017200L;

        // Convert epoch seconds to LocalDateTime
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneId.systemDefault());

        // Print the result
        System.out.println("LocalDateTime: " + dateTime);
    }
}