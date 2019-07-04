package utils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import javax.annotation.Nullable;

/**
 * This class sets the event time to the value create date of the comments (not corrupt)
 */
public class GetterEventTImeAndWatermark implements AssignerWithPunctuatedWatermarks<ObjectNode> {
    private long timestampStreamCompliant=0;

    public GetterEventTImeAndWatermark() {
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(ObjectNode jsonNodes, long l) {
        return new Watermark(l);
    }

    @Override
    public long extractTimestamp(ObjectNode jsonNodes, long l) {
        long timestamp;
        try {
            timestamp = jsonNodes.get("value").get("createDate").asLong();
            timestampStreamCompliant = timestamp;
        } catch (Exception e){
            return timestampStreamCompliant; // RETURN TO PREVIOUS TIMESTAMP COMPLIANT
        }

        return timestampStreamCompliant;
    }
}
