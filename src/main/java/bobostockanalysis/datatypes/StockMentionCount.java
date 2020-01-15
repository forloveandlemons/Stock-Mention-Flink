package bobostockanalysis.datatypes;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class StockMentionCount implements Serializable {

    public String stock;
    public LocalDateTime ts;
    public long count;

    public StockMentionCount() {

    }


    public StockMentionCount(String stock, LocalDateTime ts, long count) {
        this.stock = stock;
        this.ts = ts;
        this.count = count;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(stock).append(",");
        sb.append(ts.toString()).append(",");
        sb.append(count).append(",");

        return sb.toString();
    }

    public static StockMentionCount fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 2) {
            throw new RuntimeException("Invalid record: " + line);
        }

        StockMentionCount smc;

        try {
            String stock = "AMZN";
            LocalDateTime ts = LocalDateTime.parse(tokens[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            long value = Long.parseLong(tokens[1]);
            smc = new StockMentionCount(stock, ts, value);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return smc;
    }

    public LocalDateTime getEventTime() {
        return this.ts;
    }

    public void setTs(LocalDateTime ts) {
        this.ts = ts;
    }

    public void setCount(Long count) {
        this.count = count;
    }

}
