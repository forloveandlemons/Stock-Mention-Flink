package bobostockanalysis.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.StreamSupport;

public class SplitPathUtil {
    public static String[] splitPath(String pathString) {
        Path path = Paths.get(pathString);
        return StreamSupport.stream(path.spliterator(), false).map(Path::toString)
                .toArray(String[]::new);
    }

    public static String extractStockFromPath(String[] a) {
        String last = a[a.length - 1];
        String stock = last.substring("Twitter_volume_".length(), last.indexOf(".csv"));
        return stock;
    }
}