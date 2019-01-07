package kafkahbaseconnect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;



public class HbaseSinkConnector extends SinkConnector {
    String hbasetablename;
    Map<String, String> connectorprops;

    @Override
    public String version() {
        return "Ver.1";
    }

    @Override
    public void start(Map<String, String> props) {
        hbasetablename = props.get("hbasetablename");
        connectorprops = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HbaseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxtasks) {
        ArrayList<Map<String,String>> configs = new ArrayList<Map<String,String>>();
        for ( int i = 0; i < maxtasks ; i++) {
            configs.add(connectorprops);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

}