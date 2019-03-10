package com.amazonaws.services.kinesisanalytics;

/**
 * Stats POJO class for aggregating a value while using Flink AggregateFunction / ProcessWindowFunction
 */
public class Stats {
    // use this to avoid any serialization deserialization used within Flink
    public static final long serialVersionUID = 102L;

    public Stats() {
    }

    public Stats(Double min, Double max, Double count, Double sum) {
        this.setMin(min);
        this.setMax(max);
        this.setSum(sum);
        this.setCount(count);
        if (count > 0) {
            this.setAvg(sum / count);
        } else {
            // this should be NaN, right now for sample code using 0
            this.setAvg(0.0);
        }

    }

    private Double min;
    private Double max;
    private Double avg;
    private Double count;
    private Double sum;

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMin() {
        return min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }


    public String toString() {

        if( this.count>0 ) {
            return "STATS: min: " + min + " max: " + max + " count: " + count + " avg: " + avg ;
        }
        else {
            return "STATS: empty";
        }
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

}