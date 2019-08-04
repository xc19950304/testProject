package io.openmessaging.object;

import java.util.ArrayList;

public class Block {
    private long startOffset;
    private long tmin;
    private long tmax;
    private long amin;
    private long amax;

    private long sum;
    private String queueName;

    private ArrayList<Page> pageList;

    public Block()
    {
        pageList= new ArrayList<>();
        this.sum = 0;
    }

    public Block(long tmin, long tmax, long amin, long amax)
    {
        this.tmin = tmin;
        this.tmax = tmax;
        this.amin = amin;
        this.amax = amax;
        this.sum = 0;
        pageList= new ArrayList<>();
    }

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

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public void addSum(long sum) {
        this.sum += sum;
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

    public ArrayList<Page> getPageList() {
        return pageList;
    }

    public void setPageList(ArrayList<Page> pageList) {
        this.pageList = pageList;
    }

    public void addPage(Page page)
    {
        pageList.add(page);
    }
}
