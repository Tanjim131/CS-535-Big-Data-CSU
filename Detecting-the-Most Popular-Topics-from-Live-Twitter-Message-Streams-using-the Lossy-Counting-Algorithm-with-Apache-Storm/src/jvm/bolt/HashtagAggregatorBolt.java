package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.stream.Collectors;

public class HashtagAggregatorBolt extends BaseRichBolt {
    private static final int TOP_N = 100;
    private final int REQUIRED_NUMBER_OF_BOLT_INSTANCES;
    private Map<Integer, Map<String, Integer>> hashTagsByBoltInstances;
    private OutputCollector outputCollector;

    public HashtagAggregatorBolt(int instances) {
        this.REQUIRED_NUMBER_OF_BOLT_INSTANCES = instances;
        this.hashTagsByBoltInstances = new HashMap<>();
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, Integer> partialTopHashtagsFromCounterBolt = (Map<String, Integer>) tuple.getValue(0);
        int counterBoltTaskId = tuple.getInteger(1);
        updateWithPartialHashtags(counterBoltTaskId, partialTopHashtagsFromCounterBolt);

        if(hashTagsByBoltInstances.keySet().size() == REQUIRED_NUMBER_OF_BOLT_INSTANCES){
            emit();
            reset();
        }
    }

    public void emit(){
        outputCollector.emit(new Values(getTopHashtags()));
    }

    public void reset(){
        hashTagsByBoltInstances.clear();
    }

    public void updateWithPartialHashtags(int counterBoltTaskId, Map<String, Integer> partialTopHashtags){
        Map<String, Integer> boltEntry =
			hashTagsByBoltInstances.getOrDefault(counterBoltTaskId, new HashMap<>());

        partialTopHashtags
            .forEach((hashtag, targetFrequency) -> {
                int currentFrequency = boltEntry.getOrDefault(hashtag, 0);
                int updatedFrequency = currentFrequency + targetFrequency;
                boltEntry.put(hashtag, updatedFrequency);
            });

        hashTagsByBoltInstances.put(counterBoltTaskId, boltEntry);
    }

    public List<String> getTopHashtags(){
        Map<String, Integer> allHashtags =
            hashTagsByBoltInstances
                .values()
                .stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return allHashtags
            .entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
            .limit(TOP_N)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("TopGlobalHashtags"));
    }
}

