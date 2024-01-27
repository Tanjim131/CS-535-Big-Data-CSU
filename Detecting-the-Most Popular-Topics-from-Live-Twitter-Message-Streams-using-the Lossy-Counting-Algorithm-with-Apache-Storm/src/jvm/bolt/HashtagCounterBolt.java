package bolt;

import classes.Entry;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import java.util.*;
import java.util.stream.Collectors;

public class HashtagCounterBolt extends BaseRichBolt {
    private static final int TOP_N = 100;
    private final int reportIntervalInSeconds;
    private OutputCollector outputCollector;
    private Map<String, Entry> hashtagEntries;
    private int tupleCounter;
    private final int bucketSize;
    private final double epsilon;
    private final double threshold;
    private String componentId;
    private int taskId;

    public HashtagCounterBolt(double epsilon, double threshold) {
        this.reportIntervalInSeconds = 10;
	this.epsilon = epsilon;
        this.threshold = threshold;
        this.hashtagEntries = new HashMap<>();
        this.tupleCounter = 0;
        this.bucketSize = (int) Math.ceil(1.0 / epsilon);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
        if(TupleUtils.isTick(tuple)){
            this.emit();
        } else {
            tupleCounter += 1;

            String hashtag = tuple.getString(0);

            int bcurrent = (int) Math.ceil(tupleCounter * 1.0 / bucketSize);

            Entry newEntry = new Entry(0, bcurrent - 1);
            Entry currentEntry = hashtagEntries.getOrDefault(hashtag, newEntry);
            Entry updatedEntry = new Entry(currentEntry.getFrequency() + 1, currentEntry.getDelta());
            hashtagEntries.put(hashtag, updatedEntry);

            if(tupleCounter % bucketSize == 0){
                deletePhase(bcurrent);
            }
        }
    }

    public void emit(){
        Map<String, Integer> topHashtags = getTopHashtags();
        outputCollector.emit(new Values(topHashtags, taskId));
    }

    public Map<String, Integer> getTopHashtags(){
        return hashtagEntries
            .entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
            .limit(TOP_N)
            .filter(e -> e.getValue().getFrequency() > ((this.threshold - this.epsilon) * this.tupleCounter))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().getFrequency()
                ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("TopLocalHashtags", "Task-Id"));
    }

    public void deletePhase(int bcurrent){
        hashtagEntries.values().removeIf(e -> e.getFrequency() + e.getDelta() <= bcurrent);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, reportIntervalInSeconds);
        return conf;
    }
}

