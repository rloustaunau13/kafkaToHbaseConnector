package kafkahbaseconnect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import java.io.IOException;

import org.apache.log4j.Logger;

public class MyTweetClassSerDe implements Serializer, Deserializer {

    private final Logger logger = Logger.getLogger(MyTweetClassSerDe.class);

    @Override
    public void configure(Map map, boolean bln) {
    }

    @Override
    public byte[] serialize(String string, Object t) {
        ObjectMapper objectMapper = new ObjectMapper();
        String json_bytes = null;
        try {
            json_bytes = objectMapper.writeValueAsString((MyTweetClass)t);
        } catch (JsonProcessingException ex) {
            logger.error("Error in kafkahbaseconnect.MyTweetClassSerDe.serialize.." + ex.getMessage());
        }
        return (json_bytes != null) ? json_bytes.getBytes() : null;
    }

    @Override
    public void close() {
    }

    @Override
    public Object deserialize(String string, byte[] bytes) {

        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MyTweetClass tweet_obj = null;
        try {
            tweet_obj = (MyTweetClass) objectMapper.readValue(bytes, MyTweetClass.class);
        } catch (IOException ex) {
            logger.error("Error in kafkahbaseconnect.MyTweetClassSerDe.deSerialize.." + ex.getMessage());
        }

        return tweet_obj;
    }
}



