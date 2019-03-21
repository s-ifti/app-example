package com.amazonaws.services.kinesisanalytics.sinks;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * A custom table output that
 * will write output to kinesis and will also write few min/max/count metric values to
 * Cloudwatch
 *
 */
public class CustomTableSink implements AppendStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private FlinkKinesisProducer<String> kinesisProducerSink;
    private CloudwatchMetricSink cloudwatchMetricSink;


    public CustomTableSink(FlinkKinesisProducer<String> sink,
                           CloudwatchMetricSink cloudwatchMetricSink) {
        this.kinesisProducerSink = sink;
        this.cloudwatchMetricSink = cloudwatchMetricSink;
    }

    private CustomTableSink(String[] fieldNames,
                            TypeInformation<?>[] fieldTypes,
                            FlinkKinesisProducer<String> sink,
                            CloudwatchMetricSink cloudwatchMetricSink) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.kinesisProducerSink = sink;
        this.cloudwatchMetricSink = cloudwatchMetricSink;

    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {

        //write metrics to CW

        dataStream.map(row ->
                Arrays.asList(
                        new CloudwatchMetricTupleModel(
                                /* app name */
                                row.getField(0).toString(),
                                "min",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(3).toString())
                        ),
                        new CloudwatchMetricTupleModel(
                                /* app name */
                                row.getField(0).toString(),
                                "max",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(4).toString())
                        ),
                        new CloudwatchMetricTupleModel(
                                /* app name */
                                row.getField(0).toString(),
                                "count",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(5).toString())
                        )

            )
        )
        .startNewChain()
        .returns(TypeInformation.of(new TypeHint<List<CloudwatchMetricTupleModel>>(){}))
        .addSink(this.cloudwatchMetricSink)
        .name("cwMetricOutputAAA");

        //write to kinesis
        /*
          convert row to csv string before adding kinesis sink to it
          note: default Row toString returns a simple csv formatted string
          the value is not using full csv standard
          if dataset requires escaping use a csv formatter
         */
        dataStream.map(row -> row.toString()).startNewChain()
                .addSink(this.kinesisProducerSink).name("kinesisOutputBBB");
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return TypeInformation.of(new TypeHint<Row>() {});
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        return new CustomTableSink(fieldNames, fieldTypes, kinesisProducerSink, cloudwatchMetricSink);
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return this.fieldTypes;
    }



    static Timestamp fromStringTimestamp(String ts) {
        Timestamp timestamp = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            Date parsedDate = dateFormat.parse(ts);
            timestamp = new Timestamp(parsedDate.getTime());
        } catch (Exception e) {
        }
        return timestamp;
    }

}
