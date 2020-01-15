package bobostockanalysis.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import bobostockanalysis.util.SplitPathUtil;


public class StockMentionCountSource implements SourceFunction<String> {

    private final String dataFilePath;
    private volatile boolean isRunning = true;
    private transient BufferedReader reader;
    private String stock;

    public StockMentionCountSource(String stockName) {
        this.dataFilePath = String.format("/Users/xueyingwen/Downloads/nab/realTweets/realTweets/Twitter_volume_%s.csv", stockName);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        stock = SplitPathUtil.extractStockFromPath(SplitPathUtil.splitPath(dataFilePath));
        reader = new BufferedReader(new FileReader(dataFilePath));
        generateStream(sourceContext);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }


    private void generateStream(SourceContext<String> sourceContext) throws Exception {
        String line;
        ArrayList<String> l = new ArrayList<>();
        while (reader.ready() && (line = reader.readLine()) != null) {
            l.add(stock + "," + line);
        }

        while (isRunning) {
            for (int i = 1; i < l.size(); i++) {
                sourceContext.collect(l.get(i));
                Thread.sleep(1);
            }
        }
    }
//
//    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
//        long dataDiff = eventTime - dataStartTime;
//        return servingStartTime + (dataDiff / this.servingSpeed);
//    }



}
