package com.amazonaws.services.kinesisanalytics.converters;

import com.amazonaws.services.kinesisanalytics.AppModel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

// define a table source with a processing attribute
public class AppModelTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute {
    String[] names = new String[] {"appName" , "appId", "version"};
    TypeInformation[] types = new TypeInformation[] {Types.STRING, Types.STRING, Types.INT};

    DataStream<Row> inputStream;
    public AppModelTableSource(DataStream<Row> inputStream) {
        this.inputStream = inputStream;

    }
    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW_NAMED(names, types);
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(names, types);
    }

    @Override
    public String explainSource() {
        return names.toString();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return inputStream;
    }

    @Override
    public String getProctimeAttribute() {
        // field with this name will be appended as a third field
        return "myProcTime";
    }
}
