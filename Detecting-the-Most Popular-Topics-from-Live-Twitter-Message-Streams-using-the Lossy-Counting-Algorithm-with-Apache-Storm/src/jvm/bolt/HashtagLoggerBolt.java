package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import util.HashtagLogger;

import java.util.List;
import java.util.Map;

public class HashtagLoggerBolt extends BaseRichBolt {
    private final String hashtagsLogFileName;

    public HashtagLoggerBolt(String hashtagsLogFileName) {
        this.hashtagsLogFileName = hashtagsLogFileName;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        List<String> topHashtags = (List<String>) tuple.getValue(0);
        HashtagLogger.logTopHashtags(topHashtags, this.hashtagsLogFileName);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

