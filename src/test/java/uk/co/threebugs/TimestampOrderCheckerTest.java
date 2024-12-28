package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TimestampOrderCheckerTest {

    private final TimestampOrderChecker checker = new TimestampOrderChecker();

    @TempDir
    Path tempDir;

    @Test
    void testValidTimestamps() throws IOException {
        // Create a temporary file with valid timestamp order
        Path validFile = tempDir.resolve("valid_timestamps.csv");
        Files.write(validFile, """
                    Timestamp,Open,High,Low,Close,Volume
                    1325412060.0,458000000,458000000,458000000,458000000,0.0
                    1325412120.0,458000000,458000000,458000000,458000000,0.0
                    1325412180.0,458000000,458000000,458000000,458000000,0.0
                    1325412240.0,458000000,458000000,458000000,458000000,0.0
                    1325412300.0,458000000,458000000,458000000,458000000,0.0
                """.stripIndent().getBytes());

        // Ensure no exception is thrown for valid data
        assertDoesNotThrow(() -> checker.checkTimestampOrder(validFile));
    }

    @Test
    void testInvalidTimestamps() throws IOException {
        // Create a temporary file with invalid timestamp order
        Path invalidFile = tempDir.resolve("invalid_timestamps.csv");
        Files.write(invalidFile, """
                    Timestamp,Open,High,Low,Close,Volume
                    1325412060.0,458000000,458000000,458000000,458000000,0.0
                    1325412240.0,458000000,458000000,458000000,458000000,0.0
                    1325412120.0,458000000,458000000,458000000,458000000,0.0
                    1325412300.0,458000000,458000000,458000000,458000000,0.0
                """.stripIndent().getBytes());

        // Expect an exception to be thrown for invalid data
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> checker.checkTimestampOrder(invalidFile));

        // Check the exception message for expected details
        assertTrue(exception.getMessage().contains("Timestamps are out of order"));

    }

    @Test
    void testEmptyFile() throws IOException {
        // Create an empty temporary file
        Path emptyFile = tempDir.resolve("empty_file.csv");
        Files.createFile(emptyFile);

        // Expect an exception for the empty file
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> checker.checkTimestampOrder(emptyFile));

        // Check exception message
        assertEquals("The input file is empty!", exception.getMessage());
    }

    @Test
    void testFileWithHeaderOnly() throws IOException {
        // Create a file with only header (no data rows)
        Path headerOnlyFile = tempDir.resolve("header_only_file.csv");
        Files.write(headerOnlyFile, "Timestamp,Open,High,Low,Close,Volume".getBytes());

        // No exception should occur for header-only, as there's nothing to validate
        assertDoesNotThrow(() -> checker.checkTimestampOrder(headerOnlyFile));
    }
}