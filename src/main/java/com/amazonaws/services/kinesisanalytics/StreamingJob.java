/*
 * Flink App Example
 * The app reads stream of app events and check min max and count of events reported
 *
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.converters.JsonToAppModelStream;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.sinks.CloudwatchMetricSink;
import com.amazonaws.services.kinesisanalytics.sinks.CloudwatchMetricTupleModel;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * A sample flink stream processing job.
 *
 * The app reads stream of app events and calculate min, max, and count on version reported
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

    private static String VERSION = "1.1.4";
    private static String DEFAULT_REGION = "us-east-1";
    private static int DEFAULT_PARALLELISM = 4;

    private static Properties appProperties = null;

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LOG.warn("Starting Kinesis Analytics App Example > Version " + VERSION);

        appProperties = initRuntimeConfigProperties();

        // get input stream name from App properties
        String inputStreamName = getAppProperty("inputStreamName", "");

        if (StringUtils.isBlank(inputStreamName)) {
            LOG.error("inputStreamName should be pass using AppProperties config within create-application API call");
            throw new Exception("inputStreamName should be pass using AppProperties config within create-application API call, aborting ...");
        }

        // get output stream name from App properties
        String outputStreamName = getAppProperty("outputStreamName", "");

        if (StringUtils.isBlank(outputStreamName)) {
            LOG.error("outputStreamName should be pass using AppProperties config within create-application API call");
            throw new Exception("outputStreamName should be pass using AppProperties config within create-application API call, aborting ...");
        }

        // use a specific input stream name
        String region = getAppProperty("region", DEFAULT_REGION);

        LOG.warn("Starting Kinesis Analytics App Sample using " +
                        "inputStreamName {} outputStreamName {} region {} parallelism {}",
                        inputStreamName, outputStreamName, region, env.getParallelism());

        String metricTag = getAppProperty("metricTag", "NullMetricTag");
        int numMaxQueries = getAppPropertyInt("numQueries", 1);
        // use processing time for Time windows
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Add kinesis source
        // Notes: input data stream is a json formatted string
        DataStream<String> inputStream = getInputDataStream(env, inputStreamName, region);

        // Add kinesis output
        FlinkKinesisProducer<String> kinesisOutputSink = getKinesisOutputSink(outputStreamName, region);


        //convert json string to AppModel stream using a helper class
        DataStream<AppModel> inputAppModelStream = JsonToAppModelStream.convert(inputStream);

        for (int q = 0; q < numMaxQueries; q++) {
            SingleOutputStreamOperator<Tuple2<String, Stats>> output = inputAppModelStream
                    .map(appEvent -> new Tuple2<>(appEvent.getAppName(), appEvent)
                    ).returns(TypeInformation.of(new TypeHint<Tuple2<String, AppModel>>() {
                    }))
                    .keyBy(t -> t.f0)
                    .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.seconds(60))
                    //calc stats for last time window
                    .aggregate(new StatsAggregate(), new MyProcessWindowFunction())
                    .name("stats_TimeWindow_q" + q)
                    .returns(TypeInformation.of(new TypeHint<Tuple2<String, Stats>>() {
                    }));

                    output.map(outputTuple ->
                            Arrays.asList(
                                    new CloudwatchMetricTupleModel(
                                     /* app name */
                                            outputTuple.f0,
                                            "min",
                                            java.sql.Timestamp.from(Instant.now(Clock.systemUTC())),
                                            outputTuple.f1.getMin()
                                    ),
                                    new CloudwatchMetricTupleModel(
                                         /* app name */
                                            outputTuple.f0,
                                            "max",
                                            java.sql.Timestamp.from(Instant.now(Clock.systemUTC())),
                                            outputTuple.f1.getMax()
                                    ),
                                    new CloudwatchMetricTupleModel(
                                            /* app name */
                                            outputTuple.f0,
                                            "count",
                                            java.sql.Timestamp.from(Instant.now(Clock.systemUTC())),
                                            outputTuple.f1.getCount()
                                    )

                            )
                    ).returns(TypeInformation.of(new TypeHint<List<CloudwatchMetricTupleModel>>(){}))
                            .addSink(new CloudwatchMetricSink(metricTag + "-query-" + q));

            //use only one output kinesis stream for test
            if (q == 0) {
                //write output to kinesis stream
                output.map(row -> row.f1.toString()).addSink(kinesisOutputSink);
            }
        }
        env.execute();
    }

    private static FlinkKinesisProducer<String> getKinesisOutputSink(String outputStreamName, String region) {
        Properties producerConfig = new Properties();
        // Required configs
        producerConfig.put(AWSConfigConstants.AWS_REGION, region);
        // Optional configs
        producerConfig.put("RecordTtl", "30000");
        producerConfig.put("AggregationEnabled", "false");
        producerConfig.put("RequestTimeout", "10000");
        producerConfig.put("ThreadingModel", "POOLED");
        producerConfig.put("ThreadPoolSize", "15");

        FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(new SimpleStringSchema(), producerConfig);
        kinesis.setFailOnError(true);
        kinesis.setDefaultStream(outputStreamName);
        kinesis.setDefaultPartition("0");
        return kinesis;
    }

    private static DataStream<String> getInputDataStream(StreamExecutionEnvironment env, String inputStreamName, String region) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfigConstants.AWS_REGION, region);
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(
                inputStreamName, new SimpleStringSchema(), consumerConfig))
                .name("kinesis");
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
    private static Properties initRuntimeConfigProperties() {
        try {
            Map<String, Properties> runConfigurations = KinesisAnalyticsRuntime.getApplicationProperties();
            return runConfigurations.get("AppProperties");
        } catch (IOException var1) {
            LOG.error("Could not retrieve the runtime config properties for {}, exception {}", "AppProperties", var1);
            return null;
        }
    }


    // Helper Function definitions for direct stream based time window processing

    /**
     * The Stats accumulator is used to keep a running sum and a count.
     * see https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#incremental-window-aggregation-with-aggregatefunction
     */
    private static class StatsAggregate
            implements AggregateFunction<Tuple2<String, AppModel>, Stats, Stats> {
        @Override
        public Stats createAccumulator() {
            //start accumulator
            return new Stats( /* min start */ Double.MAX_VALUE, /* max start */ Double.MIN_VALUE, 0.0, 0.0);
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

}
