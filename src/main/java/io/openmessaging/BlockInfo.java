package io.openmessaging;

public class BlockInfo {
    private long startOffset;
    private long tmin;
    private long tmax;
    private String queueName;

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getTmin() {
        return tmin;
    }

    public void setTmin(long tmin) {
        this.tmin = tmin;
    }

    public long getTmax() {
        return tmax;
    }

    public void setTmax(long tmax) {
        this.tmax = tmax;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
