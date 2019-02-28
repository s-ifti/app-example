package com.amazonaws.services.kinesisanalytics.sinks;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Log4j table sink.
 * This class can be used to log any input or output tables to log4j
 *
 */
public class Log4jTableSink implements AppendStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private Log4jSink<Row> log4jSink;
    private String logHeader;
    private String metricTag;
    private CloudwatchMetricSink cloudwatchMetricSink;


    public Log4jTableSink(String logHeader, String metricTag) {
        this.logHeader = logHeader;
        this.metricTag = metricTag;
    }

    private Log4jTableSink(String logHeader, String metricTag, String[] fieldNames, TypeInformation<?>[] fieldTypes, Log4jSink<Row> sink) {
        this.logHeader = logHeader;
        this.metricTag = metricTag;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.log4jSink = sink;
        this.cloudwatchMetricSink = new CloudwatchMetricSink(this.metricTag);

    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream.addSink(this.log4jSink);

        /* emit metrics to CW

         */
        dataStream.map(row ->
                (List<Tuple4<String,String, Timestamp, Double>>) ImmutableList.of(
                        new Tuple4<>(
                                row.getField(0).toString(),
                                "min",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(3).toString())
                        ),
                        new Tuple4<>(
                                row.getField(0).toString(),
                                "max",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(4).toString())
                        ),
                        new Tuple4<>(
                                row.getField(0).toString(),
                                "count",
                                fromStringTimestamp(row.getField(2).toString()),
                                Double.parseDouble(row.getField(5).toString())
                        )

                )

        )
                .addSink(this.cloudwatchMetricSink);
    }


    static Timestamp fromStringTimestamp(String ts) {
        Timestamp timestamp = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            Date parsedDate = dateFormat.parse(ts);
            timestamp = new java.sql.Timestamp(parsedDate.getTime());
        } catch (Exception e) {
        }
        return timestamp;
    }


    @Override
    public TypeInformation<Row> getOutputType() {
        return TypeInformation.of(new TypeHint<Row>() {});
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Log4jSink<Row> sink = new Log4jSink<>(logHeader);
        return new Log4jTableSink(logHeader, metricTag, fieldNames, fieldTypes, sink);
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return this.fieldTypes;
    }
}
