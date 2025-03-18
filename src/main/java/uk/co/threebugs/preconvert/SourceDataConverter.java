package uk.co.threebugs.preconvert;

import java.io.IOException;
import java.nio.file.Path;

public interface SourceDataConverter {
    void convert(Path of, Path of1) throws IOException;
}
