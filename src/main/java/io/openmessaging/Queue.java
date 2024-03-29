package io.openmessaging;

import io.openmessaging.object.Block;
import io.openmessaging.object.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Queue {

    //消息大小
    public final static int MESSAGE_SIZE = 50;

    //每个读写缓冲区的消息个数
    public final static int MESSAGE_NUMBER = 100;

    //Block对应的消息总数
    public final static int FLUSH_MESSAGE_NUMBER = 1300;//65000(50*1300)

    //每个读写缓冲区的延迟消息个数
    public final static int DELAY_NUMBER = 0;//非常重要，用于保证每个块间的延迟性;

    private static Comparator<Message> comparator = (o1, o2) -> (int) (o1.getT() - o2.getT());


    //写缓冲区
    //private   ByteBuffer writeBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * MESSAGE_NUMBER);

    private   ByteBuffer readBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);

    private java.util.Queue<Message> messageBuffer = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);

    //上一个块的最大值，判断是否仍然存在延迟数据
    private long lastBlockTmax;

    //64k刷一次盘，ISOP的最优刷盘值
    private ByteBuffer flushBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);//  约等于64k

    private FileChannel channel;

    //记录当前操作块的物理偏移
    private AtomicLong writePosition;

    private ArrayList<Block> blocks = new ArrayList<Block>();

    private Block currentBlock;

    private Future<Long> flushFuture;

    private static ExecutorService flushThread = Executors.newSingleThreadExecutor();

    private String queueName;

    Queue(FileChannel channel, AtomicLong writePosition, String queueName){
        this.channel = channel;
        this.writePosition = writePosition;
        this.queueName = queueName;
        this.lastBlockTmax = 0;
        this.flushFuture = null;
    }

    private boolean thisBlockFisrtPut = true;

    public static AtomicLong atomicLong = new AtomicLong(0);

    //同一时刻一个Queue的put和flush串行执行
    public void put(Message message) {
        if(thisBlockFisrtPut){
            currentBlock = new Block(Long.MAX_VALUE,Long.MIN_VALUE,Long.MAX_VALUE,Long.MIN_VALUE);
            currentBlock.setQueueName(queueName);
            blocks.add(currentBlock);
            thisBlockFisrtPut = false;
        }
        else if (messageBuffer.size() == (MESSAGE_NUMBER + DELAY_NUMBER)) {
            flush();
        }

/*        if(atomicLong.getAndIncrement() % 10000000 == 1)
            System.out.println(queueName + " message sum:" + atomicLong + ", messageT:" + message.getT() + ", blockSize:"
                    + blocks.size() + ", tmin:" + currentBlock.getTmin() + " tmax:" + currentBlock.getTmax());*/
        //sumA += message.getA();
        messageBuffer.add(message);
    }

    //将队列中20条数据刷到64k的buffer中，做异步flush操作
    private void flush() {
        //起异步任务获取上一个块的偏移量
        if (flushFuture != null) {
            try {
                long writeLength = flushFuture.get();
                if(writeLength != -1)
                    //currentBlock.setStartOffset(writePosition.get() - writeLength);
                    blocks.get(blocks.size()-2).setStartOffset(writePosition.get() - writeLength);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            flushFuture = null;
        }
        long segmentStartT = 0;
        long segmentEndT = 0;
        long segmentStartA = Long.MAX_VALUE;
        long segmentEndA = Long.MIN_VALUE;
        long currentPageSumA = 0;

        for (int i = 0; i < MESSAGE_NUMBER; i++) {
            Message message = messageBuffer.poll();

            long a = message.getA();
            currentPageSumA += a;
            flushBuffer.putLong(message.getT());
            flushBuffer.putLong(a);
            flushBuffer.put(message.getBody());
            if (i == 0 ) {
                segmentStartT = message.getT();
            }
            else if (i == MESSAGE_NUMBER - 1) {
                segmentEndT = message.getT();
            }

            segmentStartA = Math.min(a,segmentStartA);
            segmentEndA = Math.max(a,segmentEndA);
        }

        //Page page = new Page(segmentStartT,segmentEndT,segmentStartA,segmentEndA,currentPageSumA);

        currentBlock.setTmin(Math.min(segmentStartT,currentBlock.getTmin()));
        currentBlock.setTmax(Math.max(segmentEndT,currentBlock.getTmax()));
        currentBlock.setAmin(Math.min(segmentStartA,currentBlock.getAmin()));
        currentBlock.setAmax(Math.max(segmentEndA,currentBlock.getAmax()));
        currentBlock.addSum(currentPageSumA);
        //currentBlock.addPage(page);


        if (flushBuffer.remaining() < MESSAGE_SIZE * MESSAGE_NUMBER) {

            thisBlockFisrtPut = true;

            flushFuture = flushThread.submit(() -> {
                long writeLength = -1L;
                flushBuffer.flip();
                try {
                    writeLength = channel.write(flushBuffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                this.writePosition.getAndAdd(writeLength);
                flushBuffer.clear();

                return writeLength;
            });
        }
    }

    Lock queueLock = new ReentrantLock();

    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {

        queueLock.lock();

        List<Message> result = new ArrayList<>();
        List<Message> restData = new ArrayList<>();

        int size = blocks.size();

        //最后一个block不一定刷盘，且数据存在于优先队列(必有)和flush_buffer(可能有)中
        //处理flush_buffer
        flushBuffer.flip();
        int messageNum = flushBuffer.remaining() / MESSAGE_SIZE;
        if (messageNum != 0) {
            for (int i = 0; i < messageNum; i++) {
                byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                long t = flushBuffer.getLong();
                long a = flushBuffer.getLong();
                flushBuffer.get(body);
                Message msg = new Message(a, t, body);
                if (t >= tMin && t <= tMax && a >= aMin && a <= aMax)
                    restData.add(msg);
            }
            size = size - 1;
        }

        //处理queue_buffer
        java.util.Queue<Message> tempQueue1 = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);
        while (!messageBuffer.isEmpty()) {
            Message msg = messageBuffer.poll();
            tempQueue1.offer(msg);
            long a = msg.getA();
            long t = msg.getT();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax)
                restData.add(msg);
        }
        messageBuffer = tempQueue1;

        for (int j = 0; j <= size - 1; j++) {
            long blockTmin = blocks.get(j).getTmin();
            long blockTmax = blocks.get(j).getTmax();
            if(blockTmin > tMax || blockTmax < tMin)
                continue;

            readBuffer.clear();
            try {
                channel.read(readBuffer, blocks.get(j).getStartOffset());
            } catch (IOException e) {
                e.printStackTrace();
            }
            readBuffer.flip();

            //边界Block数据 和 部分乱序的Block块的边界值刚好在查询的边界上 特殊处理
            if((tMin >= blockTmin && tMin<=blockTmax)|| (tMax >= blockTmin && tMax <=blockTmax)){
                for (int i = 0; i < FLUSH_MESSAGE_NUMBER; i++) {
                    byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                    long t = readBuffer.getLong();
                    long a = readBuffer.getLong();
                    readBuffer.get(body);
                    Message msg = new Message(a, t, body);
                    if (t >= tMin && t <= tMax && a >= aMin && a <= aMax)
                        result.add(msg);
                }
            }
            else {
                for (int i = 0; i < FLUSH_MESSAGE_NUMBER; i++) {
                    byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                    long t = readBuffer.getLong();
                    long a = readBuffer.getLong();
                    readBuffer.get(body);
                    Message msg = new Message(a, t, body);
                    if ( a >= aMin && a <= aMax)
                        result.add(msg);
                }
            }
        }
/*        if (result != null && result.size() > 0)
            System.out.println("[Queue] " + queueName + " disk data filter end"
                    + " t:[" + result.get(0).getT() + "," + result.get(result.size() - 1).getT() + "]"
                    + " a:[" + result.get(0).getA() + "," + result.get(result.size() - 1).getA() + "]");
        else
            System.out.println("[Queue] " + queueName + " disk data filter end" + " null");*/
        result.addAll(restData);
        queueLock.unlock();
        return result;
    }


    public long[] getAvgMessage(long aMin, long aMax, long tMin, long tMax) {

        queueLock.lock();

        long[] result = new long[2];
        long sum = 0;
        long length = 0;

        int size = blocks.size();

        //最后一个block不一定刷盘，且数据存在于优先队列(必有)和flush_buffer(可能有)中
        //处理flush_buffer
        flushBuffer.flip();
        int messageNum = flushBuffer.remaining() / MESSAGE_SIZE;
        if (messageNum != 0) {
            for (int i = 0; i < messageNum; i++) {
                byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                long t = flushBuffer.getLong();
                long a = flushBuffer.getLong();
                flushBuffer.get(body);
                if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                    sum += a;
                    length ++;
                }
            }
            size = size - 1;
        }

        //处理queue_buffer
        java.util.Queue<Message> tempQueue1 = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);
        while (!messageBuffer.isEmpty()) {
            Message msg = messageBuffer.poll();
            tempQueue1.offer(msg);
            long a = msg.getA();
            long t = msg.getT();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                sum += a;
                length ++;
            }
        }
        messageBuffer = tempQueue1;

        for (int j = 0; j <= size - 1; j++) {
            long blockTmin = blocks.get(j).getTmin();
            long blockTmax = blocks.get(j).getTmax();
            long blockAmin = blocks.get(j).getAmin();
            long blockAmax = blocks.get(j).getAmax();
            if(blockTmin > tMax || blockTmax < tMin)
                continue;


            //边界Block数据 和 部分乱序的Block块的边界值刚好在查询的边界上 特殊处理
            if((tMin >= blockTmin && tMin<=blockTmax)|| (tMax >= blockTmin && tMax <=blockTmax)
            /*|| (aMin >= blockAmin && aMin<=blockAmax)|| (aMax >= blockAmin && aMax <=blockAmax) */ ){
               //System.out.println( queueName + " " + Thread.currentThread().getName() + " read disk1：" + "..."+j);
                readBuffer.clear();
                try {
                    channel.read(readBuffer, blocks.get(j).getStartOffset());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                readBuffer.flip();
                for (int i = 0; i < FLUSH_MESSAGE_NUMBER; i++) {
                    byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                    long t = readBuffer.getLong();
                    long a = readBuffer.getLong();
                    readBuffer.get(body);
                    if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                        sum += a;
                        length ++;
                    }
                }
            }

            else {
                if(blockAmin >= aMin && blockAmax <= aMax) {
                    //System.out.println(queueName + " " + Thread.currentThread().getName()+  " read memory："  + "..." + j);
                    sum += blocks.get(j).getSum();
                    length += FLUSH_MESSAGE_NUMBER;
                }
                else
                {
                    //System.out.println(queueName + " " + Thread.currentThread().getName()+  " read disk2："  + "..." + j);
                    readBuffer.clear();
                    try {
                        channel.read(readBuffer, blocks.get(j).getStartOffset());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    readBuffer.flip();
                    for (int i = 0; i < FLUSH_MESSAGE_NUMBER; i++) {
                        byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                        long t = readBuffer.getLong();
                        long a = readBuffer.getLong();
                        readBuffer.get(body);
                        if (a >= aMin && a <= aMax) {
                            sum += a;
                            length ++;
                        }
                    }
                }
            }
        }
        result[0] = sum;
        result[1] = length;

        queueLock.unlock();

        return result;
    }
}
