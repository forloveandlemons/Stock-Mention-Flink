package bobostockanalysis.connector;

import bobostockanalysis.datatypes.StockMentionCount;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;


public class StockMentionCountSchema implements DeserializationSchema<StockMentionCount>, SerializationSchema<StockMentionCount> {

    @Override
    public StockMentionCount deserialize(byte[] bytes) throws IOException {
        return StockMentionCount.fromString(new String(bytes));
    }

    @Override
    public byte[] serialize(StockMentionCount smc) {
        return smc.toString().getBytes();
    }

    @Override
    public TypeInformation<StockMentionCount> getProducedType() {
        return TypeExtractor.getForClass(StockMentionCount.class);
    }

    // Method to decide whether the element signals the end of the stream.
    // If true is returned the element won't be emitted.
    @Override
    public boolean isEndOfStream(StockMentionCount smc) {
        return false;
    }
}