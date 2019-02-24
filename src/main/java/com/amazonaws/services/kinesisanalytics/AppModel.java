package com.amazonaws.services.kinesisanalytics;

import java.sql.Timestamp;

/**
 * App POJO class
 */
public class AppModel {
    // use this to avoid any serialization deserialization used within Flink
    public static final long serialVersionUID = 40L;

    public AppModel() {

    }

    public AppModel(String appName, String appId, Integer version, Timestamp timestamp) {
        this.appName = appName;
        this.appId = appId;
        this.version = version;
        this.timestamp = timestamp;

    }

    private String appName;
    private String appId;
    private Integer version;
    private Timestamp timestamp;

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppId() {
        return this.appId;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppName() {
        return this.appName;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Integer getVersion() {
        return this.version;
    }

    public String toString() {
        return "APP: " + appName + " TIMESTAMP: " + timestamp + " appId: " + appId + " Version: " + version;
    }


}