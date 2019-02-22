/*
 * Flink App Example
 * The app reads stream of app events and check min max and count of events reported
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * A sample flink stream processing job.
 *
 * The app reads stream of app events and calculate min, max, and count
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    private static Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private static String VERSION = "1.0.5";
    private static String DEFAULT_REGION = "us-east-1";
    private static int DEFAULT_PARALLELISM = 4;

    private static Properties appProperties = null;

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LOG.info("Starting Kinesis Analytics App Example > Version " + VERSION);

        appProperties = getRuntimeConfigProperties();

        // use a specific input stream name
        String streamName = getAppProperty("inputStreamName", "");

        if (StringUtils.isBlank(streamName)) {
            LOG.error("inputStreamName should be pass using AppProperties config within create-application API call");
            throw new Exception("inputStreamName should be pass using AppProperties config within create-application API call, aborting ...");
        }

        // use a specific input stream name
        String region = getAppProperty("region", DEFAULT_REGION);

        int parallelism = getAppPropertyInt("parallelism", DEFAULT_PARALLELISM);

        String metricTag = getAppProperty("metricTag", "None");

        LOG.info("Starting Kinesis Analytics App Sample using parallelism {} " +
                        " stream {} region {} metricTag {} ",
                parallelism, streamName, region, metricTag);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(parallelism);

        // Add kinesis as source
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfigConstants.AWS_REGION, region);
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        DataStream<String> inputStream = env.addSource(new FlinkKinesisConsumer<>(
                streamName, new SimpleStringSchema(), consumerConfig))
                .name("kinesis");


        // an example App stream processing job graph
        DataStream<Tuple2<String, AppModel>> sampleApp =
                //start with inputStream
                inputStream
                        //process JSON and return a model POJO class
                        .map(c -> {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode jsonNode = mapper.readValue(c, JsonNode.class);
                            Timestamp timestamp = null;
                            try {
                                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                                Date parsedDate = dateFormat.parse(jsonNode.get("dataTimestamp").asText());
                                timestamp = new java.sql.Timestamp(parsedDate.getTime());
                            } catch (Exception e) {
                                LOG.error("Error processing timestamp " + e.toString());
                            }
                            int version = 0;
                            try {
                                version = jsonNode.get("version").asInt();
                            } catch (Exception e) {
                            }

                            return new AppModel(
                                    jsonNode.get("appName").asText(),
                                    jsonNode.get("appId").asText(),
                                    version,
                                    timestamp
                            );

                        }).
                        returns(AppModel.class)
                        .name("map_AppModelPOJO")
                        //assign timestamp for time window processing
                        .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                        .name("timestamp")
                        //create tuple of App Name
                        //log input app object
                        .map(appEvent -> {
                                    LOG.info("App: " + appEvent.toString());
                                    return new Tuple2<>(appEvent.getAppName(), appEvent);
                                }
                        ).returns(TypeInformation.of(new TypeHint<Tuple2<String, AppModel>>() {
                }))
                        .name("map_AppName_Version_Tuple");

        DataStream<Tuple2<String, Stats>> statsOutput = sampleApp
                //partition by app name (e.g. facebook, googledeadplus, chime, etc....)
                .keyBy(x -> x.f0)
                .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.seconds(30))
                //calc stats for last 30 seconds window
                .aggregate(new StatsAggregate(), new MyProcessWindowFunction())
                .name("stats_30Sec")
                .map(stats -> {
                    LOG.info("APP {}, Stats {} ", stats.f0, stats.f1.toString());
                    return stats;
                }).name("map_logToCW");


        statsOutput.print()
                .name("stdout");

        env.execute();
    }


    private static String getAppProperty(String name, final String defaultValue) {
        String value = defaultValue;
        if (appProperties != null) {
            value = appProperties.getProperty(name);
            value = StringUtils.isBlank(value) ? defaultValue : value;
        }
        return value;
    }

    private static int getAppPropertyInt(String name, final int defaultIntValue) {
        String value = getAppProperty(name, "" + defaultIntValue);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.error("invalid string value {} given for property {} using default value ", value, name);
            return defaultIntValue;
        }
    }

    // helper method to return runtime properties for Property Group AppProperties
    public static Properties getRuntimeConfigProperties() {
        try {
            Map<String, Properties> runConfigurations = KinesisAnalyticsRuntime.getApplicationProperties();
            return (Properties) runConfigurations.get("AppProperties");
        } catch (IOException var1) {
            LOG.error("Could not retrieve the runtime config properties for {}, exception {}", "AppProperties", var1);
            return null;
        }
    }


    // Helper Function definitions for time window processing

    /**
     * The Stats accumulator is used to keep a running sum and a count.
     * see https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#incremental-window-aggregation-with-aggregatefunction
     */
    private static class StatsAggregate
            implements AggregateFunction<Tuple2<String, AppModel>, Stats, Stats> {
        @Override
        public Stats createAccumulator() {
            return new Stats(0.0, 0.0, 0.0, 0.0);
        }

        @Override
        public Stats add(Tuple2<String, AppModel> value, Stats accumulator) {
            return new Stats(
                    Math.min(accumulator.getMin(), value.f1.getVersion()),
                    Math.max(accumulator.getMax(), value.f1.getVersion()),
                    accumulator.getCount() + 1L,
                    accumulator.getSum() + value.f1.getVersion()
            );
        }

        @Override
        public Stats getResult(Stats accumulator) {
            return accumulator;
        }

        @Override
        public Stats merge(Stats a, Stats b) {
            return new Stats(
                    Math.min(a.getMin(), b.getMin()),
                    Math.max(a.getMax(), b.getMax()),
                    a.getCount() + b.getCount(),
                    a.getSum() + b.getSum()
            );
        }
    }

    /**
     * Proessing window to return stats using a string key.
     * see https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#incremental-window-aggregation-with-aggregatefunction
     */
    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Stats, Tuple2<String, Stats>, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<Stats> aggregates,
                            Collector<Tuple2<String, Stats>> out) {
            Stats stats = aggregates.iterator().next();
            out.collect(new Tuple2<>(key, stats));
        }
    }


    // for generating timestamp and watermark, required for using any time Window processing
    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<AppModel> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public long extractTimestamp(AppModel app, long previousElementTimestamp) {
            return app.getTimestamp().toInstant().toEpochMilli();
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}
