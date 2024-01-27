package topology;

import bolt.HashtagAggregatorBolt;
import bolt.HashtagCounterBolt;
import bolt.HashtagLoggerBolt;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spout.TwitterSampleSpout;

public class TopHashtagsParallelTopology extends ConfigurableTopology {
    private static final int NUMBER_OF_WORKERS = 4;

    public static void main(String[] args) {
        ConfigurableTopology.start(new TopHashtagsParallelTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        conf.setNumWorkers(NUMBER_OF_WORKERS);

        String topologyName = "TopHashtagsParallelTopology";
        String hashtagsFileName = "/tmp/hashtags.txt";
        String hashtagsLogFileName = "/tmp/hashtags_log.txt";

        if (args.length >= 1) {
            topologyName = args[0];
            hashtagsFileName = args[1];
            hashtagsLogFileName = args[2];
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String spoutId = "hashtag-generator-spout";
        String counterId = "hashtag-counter-bolt";
        String aggregatorId = "hashtag-aggregator-bolt";
        String printerId = "hashtag-reporter-bolt";

        topologyBuilder
            .setSpout(spoutId, new TwitterSampleSpout(hashtagsFileName), 1);

        topologyBuilder
            .setBolt(counterId, new HashtagCounterBolt(0.02, 0.02), 4)
            .fieldsGrouping(spoutId, new Fields("hashtag"));

        topologyBuilder
            .setBolt(aggregatorId, new HashtagAggregatorBolt(4), 1)
            .globalGrouping(counterId);

        topologyBuilder
            .setBolt(printerId, new HashtagLoggerBolt(hashtagsLogFileName), 1)
            .globalGrouping(aggregatorId);

        return submit(topologyName, conf, topologyBuilder);
    }
}

