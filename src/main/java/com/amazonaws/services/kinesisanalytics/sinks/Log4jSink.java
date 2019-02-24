/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A log4j logging sink (note that this is a simple sink where checkpoint is not relevant),
 * this is similar to print() instead of console output it write logs to log4j, which
 * depending on backend can also be sent to cloudwatch
 */
public class Log4jSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static Logger LOG = LoggerFactory.getLogger(Log4jSink.class);


    public Log4jSink() {

    }

    @Override
    public void invoke(T document)  {
        if(document != null) {
            LOG.info("Log4jSink: " + document.toString());
        }
    }

    @Override
    public void open(Configuration configuration) throws Exception{
        super.open(configuration);
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //nothing to do for state
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        //nothing to do for state
    }


}
