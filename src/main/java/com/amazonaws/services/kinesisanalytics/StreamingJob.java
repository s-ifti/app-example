/*
 * Flink App Example
 * The app reads stream of app events and check min max and count of events reported
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.converters.CsvToAppModelStream;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.sinks.Log4jTableSink;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;
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

    private static final long TIME_WINDOW_IN_SECONDS = 60L;

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

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataStream<AppModel> inputAppModelStream = CsvToAppModelStream.convert(inputStream);

        Table table = tableEnv.fromDataStream(inputAppModelStream, "appName,timestamp,appId,version,timestamp.rowtime");
        Table output = table
                .window(Tumble.over("1.minutes").on("rowtime").as("w"))
                .groupBy("w, appName")
                .select("appName, w.start, w.end, version.min as minVersion, version.max as maxVersion, version.count as versionCount ");
        output.writeToSink(new Log4jTableSink<Row>());

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

}
