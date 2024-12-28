package uk.co.threebugs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class LowHighColumnAdderTest {

    private final LowHighColumnAdder columnAdder = new LowHighColumnAdder();

    @Test
    void testProcessRow_shouldHandleFirstRow() {
        String previousRow = null;  // No previous row for the first row
        String currentRow = "2023-10-01,open,close,5,3";

        String result = columnAdder.processRow(previousRow, currentRow, 3, 4);

        // Verify that fixedLow = Low and fixedHigh = high
        assertThat(result).isEqualTo("2023-10-01,open,close,5,3,3,5");
    }

    @Test
    void testProcessRow_shouldHandleSubsequentRows() {
        String previousRow = "2023-10-01,open,close,5,3";
        String currentRow = "2023-10-02,open,close,4,2";

        String result = columnAdder.processRow(previousRow, currentRow, 3, 4);

        // Verify that fixedLow and fixedHigh are computed correctly
        assertThat(result.trim()).isEqualTo("2023-10-02,open,close,4,2,3,4");
    }

    @Test
    void testProcessRow_shouldThrowExceptionForMalformedRow() {
        String previousRow = "2023-10-01,open,close,5,3";
        String malformedRow = "2023-10-02,open,close";

        assertThatThrownBy(() -> columnAdder.processRow(previousRow, malformedRow, 3, 4))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .withFailMessage("Malformed row should throw an exception");
    }

    @Test
    void testProcessRow_shouldThrowExceptionForInvalidNumberFormat() {
        String previousRow = "2023-10-01,open,close,5,3";
        String invalidRow = "2023-10-02,open,close,high,low";

        assertThatThrownBy(() -> columnAdder.processRow(previousRow, invalidRow, 3, 4))
                .isInstanceOf(NumberFormatException.class)
                .withFailMessage("Invalid number format should throw an exception");
    }

    @Test
    void testProcessRow_shouldHandleEmptyPreviousRow() {
        String currentRow = "2023-10-01,open,close,5,3";

        String result = columnAdder.processRow(null, currentRow, 3, 4);

        assertThat(result).isEqualTo("2023-10-01,open,close,5,3,3,5");
    }

    @Test
    void testAddFixedLowAndHighColumns_shouldProcessFileSuccessfully(@TempDir Path tempDir) throws IOException {
        // Input file
        Path inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
                Date,open,close,high,Low
                2023-10-01,open,close,5,3
                2023-10-02,open,close,4,2""");

        // Output file
        Path outputFile = tempDir.resolve("output.csv");

        // Process file
        columnAdder.addFixedLowAndHighColumns(inputFile, outputFile);

        // Read and assert output
        String outputContent = Files.readString(outputFile);
        String expectedContent = """
                Date,open,close,high,Low,fixedLow,fixedHigh
                2023-10-01,open,close,5,3,3,5
                2023-10-02,open,close,4,2,3,4""";

        assertThat(outputContent.trim()).isEqualTo(expectedContent.trim());
    }

    @Test
    void testAddFixedLowAndHighColumns_shouldHandleEmptyFile(@TempDir Path tempDir) throws IOException {
        // Input file
        Path inputFile = tempDir.resolve("empty.csv");
        Files.createFile(inputFile);

        // Output file
        Path outputFile = tempDir.resolve("output.csv");

        // Assert that exception is thrown for an empty file
        assertThatThrownBy(() -> columnAdder.addFixedLowAndHighColumns(inputFile, outputFile))
                .isInstanceOf(IOException.class)
                .hasMessage("Input file is empty or missing a header row");
    }

    @Test
    void testAddFixedLowAndHighColumns_shouldHandleMissingColumns(@TempDir Path tempDir) throws IOException {
        // Input file
        Path inputFile = tempDir.resolve("input_missing_columns.csv");
        Files.writeString(inputFile, """
                Date,open,close
                2023-10-01,open,close
                """);

        // Output file
        Path outputFile = tempDir.resolve("output.csv");

        // Assert that processing fails due to missing columns
        assertThatThrownBy(() -> columnAdder.addFixedLowAndHighColumns(inputFile, outputFile))
                .isInstanceOf(IllegalArgumentException.class)
                .withFailMessage("Input file is missing required 'high' or 'low' columns");
    }

    @Test
    void testAddFixedLowAndHighColumns_shouldHandleMalformedRows(@TempDir Path tempDir) throws IOException {
        // Input file
        Path inputFile = tempDir.resolve("malformed.csv");
        Files.writeString(inputFile, """
                Date,open,close,high,Low
                2023-10-01,open,close,5,3
                2023-10-02,open,close""");

        // Output file
        Path outputFile = tempDir.resolve("output.csv");

        // Assert exception for malformed row
        assertThatThrownBy(() -> columnAdder.addFixedLowAndHighColumns(inputFile, outputFile))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .withFailMessage("Malformed row should throw an exception");
    }

    @Test
    void testAddFixedLowAndHighColumns_shouldWriteOnlyHeaderForHeaderOnlyFile(@TempDir Path tempDir) throws IOException {
        // Input file
        Path inputFile = tempDir.resolve("headeronly.csv");
        Files.writeString(inputFile, "Date,open,close,high,Low");

        // Output file
        Path outputFile = tempDir.resolve("output.csv");

        // Process file
        columnAdder.addFixedLowAndHighColumns(inputFile, outputFile);

        // Read and assert output
        String outputContent = Files.readString(outputFile);
        String expectedContent = "Date,open,close,high,Low,fixedLow,fixedHigh";

        assertThat(outputContent.trim()).isEqualTo(expectedContent.trim());
    }
}