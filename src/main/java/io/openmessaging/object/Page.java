package io.openmessaging.object;

import java.util.ArrayList;

public class Page {

    private long tmin;
    private long tmax;
    private long amin;
    private long amax;

    private long sum;

    public Page(long tmin, long tmax, long amin, long amax, long sum)
    {
        this.tmin = tmin;
        this.tmax = tmax;
        this.amin = amin;
        this.amax = amax;
        this.sum = sum;
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

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }
}
