package utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class DeserializationAndAddEntryTime extends JSONKeyValueDeserializationSchema {
    private ObjectMapper mapperT;

    public DeserializationAndAddEntryTime(boolean includeMetadata) {
        super(includeMetadata);
    }

    @Override
    public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        ObjectNode deserialize = super.deserialize(record);
        if (this.mapperT == null) {
            this.mapperT = new ObjectMapper();
        }

        deserialize.set("entryTimeTuple", this.mapperT.readValue(String.valueOf(System.nanoTime()), JsonNode.class));

        return deserialize;
    }

}
