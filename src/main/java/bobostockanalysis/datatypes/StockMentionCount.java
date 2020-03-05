package bobostockanalysis.datatypes;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class StockMentionCount implements Serializable {

    public String stock;
    public LocalDateTime ts;
    public Integer count;

    public StockMentionCount() {

    }


    public StockMentionCount(String stock, LocalDateTime ts, Integer count) {
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
        if (tokens.length != 3) {
            throw new RuntimeException("Invalid record: " + line);
        }

        StockMentionCount smc;

        try {
            String stock = tokens[0];
            LocalDateTime ts = LocalDateTime.parse(tokens[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            Integer value = Integer.parseInt(tokens[2]);
            smc = new StockMentionCount(stock, ts, value);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return smc;
    }

    public long getEventTime() {
        return this.ts.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public void setTs(LocalDateTime ts) {
        this.ts = ts;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public long getCount() {
        return this.count;
    }

}
