package io.openmessaging;

public class BlockInfo {
    private long startOffset;
    private long tmin;
    private long tmax;
    private long amin;
    private long amax;

    private int length;
    private long sum;
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

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public void addSum(long sum) {
        this.sum +=sum;
    }

    public long getAmin() {
        return amin;
    }

    public void setAmin(long amin) {
        this.amin = amin;
    }

    public long getAmax() {
        return amax;
    }

    public void setAmax(long amax) {
        this.amax = amax;
    }
}
