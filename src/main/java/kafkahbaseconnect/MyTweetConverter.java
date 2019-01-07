package kafkahbaseconnect;

import java.util.Map;


import kafkahbaseconnect.MyTweetClass;
import kafkahbaseconnect.MyTweetClassSerDe;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import org.apache.kafka.connect.storage.Converter;


public class MyTweetConverter implements Converter {

    @Override
    public void configure(Map<String, ?> map, boolean bln) {

    }

    @Override
    public byte[] fromConnectData(String string, Schema schema, Object o) {
        MyTweetClassSerDe tweet_serde = new MyTweetClassSerDe();
        return tweet_serde.serialize(string, o);
    }

    @Override
    public SchemaAndValue toConnectData(String string, byte[] bytes) {
        MyTweetClassSerDe tweet_serde = new MyTweetClassSerDe();
        MyTweetClass tweet_obj = (MyTweetClass) tweet_serde.deserialize(string, bytes);
        return (new SchemaAndValue(null, tweet_obj));
    }

}