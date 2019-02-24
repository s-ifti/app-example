package com.amazonaws.services.kinesisanalytics.sinks;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;

public class Log4jTableSink<T> implements AppendStreamTableSink<T> {
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private Log4jSink<T> log4jSink;


    public Log4jTableSink() {

    }

    public Log4jTableSink(String[] fieldNames, TypeInformation<?>[] fieldTypes, Log4jSink<T> sink) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.log4jSink = sink;
    }
    @Override
    public void emitDataStream(DataStream<T> dataStream) {
        dataStream.addSink(this.log4jSink);
    }

    @Override
    public TypeInformation<T> getOutputType() {
        return TypeInformation.of(new TypeHint<T>() {});
    }

    @Override
    public TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Log4jSink<T> sink = new Log4jSink<T>();
        return new Log4jTableSink<T>(fieldNames, fieldTypes, sink);
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
