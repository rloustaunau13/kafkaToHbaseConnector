package kafkahbaseconnect;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.hadoop.hbase.client.Put;
import twittersentiment.MyTweetClass;

/**
 *
 * @author kamal
 */
public class HbaseSinkTask extends SinkTask {




    Table htab;
    Connection hbaseconn;
    Configuration hbaseconf;
    SinkTaskContext taskcontext;
    private final Logger logger = Logger.getLogger(HbaseSinkTask.class);

    @Override
    public void initialize(SinkTaskContext context) {
        taskcontext = context;
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            // Get hbase tablename from the configuration
            String hbasetablename = props.get("hbasetablename");
            /* Load hbase configuration, required hbase conf files on classpath
            Then create connection and load table object.
             */
            hbaseconf = HBaseConfiguration.create();
            hbaseconn = ConnectionFactory.createConnection(hbaseconf);
            htab = hbaseconn.getTable(TableName.valueOf(hbasetablename));
        } catch (IOException ex) {
            logger.error("Error in SinkTask:start().." + ex.getMessage());
        }

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach((record) -> {
            try {
                //Create hbase put object "row" usingg record-key.
                Put p = new Put(Bytes.toBytes(record.key().toString()));
                //This is not used, offset will be worked in next code revisions
                Long offset = record.kafkaOffset();
                //Load record-value to tweet object and get tweet map to process.
                MyTweetClass tweet_obj = (MyTweetClass) record.value();
                HashMap<String, String> tweet = tweet_obj.getTweet();
                /* Add tweet values as columns in the put. This loop is one reason why
                hashmap was used to set and get tweets.
                 */
                tweet.forEach((key, value) -> {
                            // We skip adding and column where value is null
                            if (value != null) {
                                p.add("cf".getBytes(), key.getBytes(), value.getBytes());
                            }
                        }
                );
                //Insert the row to table
                htab.put(p);
                //Log the insert
                logger.info("In SinkTask.put(): record with key " + record.key().toString() + " inserted in hbase. Hbase offset is " + record.kafkaOffset());
            } catch (IOException ex) {
                logger.error("Error in SinkTask.put().." + ex.getMessage());
            }
        });
    }

    @Override
    public void stop() {
        try {
            htab.close();
            hbaseconn.close();
        } catch (IOException ex) {
            logger.error("Error in SinkTask.stop().." + ex.getMessage());
        }
    }

    @Override
    public String version() {
        return "Ver.1";
    }

}
