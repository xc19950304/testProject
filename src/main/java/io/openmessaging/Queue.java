package io.openmessaging;


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
    public final static int DELAY_NUMBER = 1000;//非常重要，用于保证每个块间的延迟性;

    private static Comparator<Message> comparator = (o1, o2) -> (int) (o1.getT() - o2.getT());


    //写缓冲区
    private   ByteBuffer writeBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * MESSAGE_NUMBER);

    private   ByteBuffer readBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);

    private java.util.Queue<Message> messageBuffer = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);

    private static java.util.Queue<Message> delayBuffer = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);

    //上一个块的最大值，判断是否仍然存在延迟数据
    private long lastBlockTmax;

    //64k刷一次盘，ISOP的最优刷盘值
    private ByteBuffer flushBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);//  约等于64k

    private FileChannel channel;

    //记录当前操作块的物理偏移
    private AtomicLong writePosition;

    private ArrayList<BlockInfo> blocks = new ArrayList<BlockInfo>();

    private BlockInfo currentBlock;

    private Future<Long> flushFuture;

    private static ExecutorService flushThread = Executors.newSingleThreadExecutor();

    private String queueName;

    Queue(FileChannel channel, AtomicLong writePosition, String queueName){
        this.channel = channel;
        this.writePosition = writePosition;
        this.queueName = queueName;
        this.lastBlockTmax = 0;
    }

    private boolean thisBlockFisrtPut = true;

    public static AtomicLong atomicLong = new AtomicLong(0);

    //同一时刻一个Queue的put和flush串行执行
    public synchronized void put(Message message) {
        if(thisBlockFisrtPut){
            currentBlock = new BlockInfo();
            currentBlock.setTmin(segmentStartT);
            currentBlock.setTmax(segmentEndT);
            currentBlock.setQueueName(queueName);
            blocks.add(currentBlock);
            thisBlockFisrtPut = false;
            //System.out.println(queueName + " block " + (blocks.size()) + " put begin" );
        }
        else if (messageBuffer.size() == (MESSAGE_NUMBER + DELAY_NUMBER)) {
            flush();
        }

        /*if(atomicLong.getAndIncrement() % 10000000 == 1)
            System.out.println(queueName + " message sum:" + atomicLong + ", messageT:" + message.getT() + ", blockSize:"
                    + blocks.size() + ", tmin:" + currentBlock.getTmin() + " tmax:" + currentBlock.getTmax());*/
        messageBuffer.add(message);
    }


    private long segmentStartT = 0;
    private long segmentEndT = 0;
    //将队列中20条数据刷到64k的buffer中，做异步flush操作
    private void flush() {
        for (int i = 0; i < MESSAGE_NUMBER; i++) {
            Message message = messageBuffer.poll();
            writeBuffer.putLong(message.getT());
            writeBuffer.putLong(message.getA());
            writeBuffer.put(message.getBody());
            if (i == 0 ) {
                segmentStartT = message.getT();
            }
            else if (i == MESSAGE_NUMBER - 1) {
                segmentEndT = message.getT();
            }
        }

        flushFuture = flushThread.submit(() -> {
            long writePosition = -1L;
            try {
                if (flushBuffer.remaining() < MESSAGE_SIZE * MESSAGE_NUMBER) {

                    lastBlockTmax = currentBlock.getTmax();
                    flushBuffer.flip();
                    writePosition = this.writePosition.get();//获取刷块时的物理地址
                    channel.write(flushBuffer);
                    this.writePosition.getAndAdd(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);
                    flushBuffer.clear();

                    thisBlockFisrtPut = true;

                    //System.out.println(" tmin：" + currentBlock.getTmin() + " tmax：" + currentBlock.getTmax() );
                    //System.out.println("----------flush To Dist------------blockIndex:"+blockIndex + queueName);
                    //System.out.println("----------flush To Dist------------writePosition:"+writePosition+ queueName);
                }
                else {
                    currentBlock.setTmin(Math.min(segmentStartT,currentBlock.getTmin()));
                    currentBlock.setTmax(Math.max(segmentEndT,currentBlock.getTmax()));
                }

                writeBuffer.flip();
                flushBuffer.put(writeBuffer);
                writeBuffer.clear();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return writePosition;
        });
        //起异步任务获取上一个块的偏移量
        if (flushFuture != null) {
            try {
                long currentBlockIndex = flushFuture.get();
                if(currentBlockIndex != -1)
                    currentBlock.setStartOffset(currentBlockIndex);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            flushFuture = null;
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
        if (result != null && result.size() > 0)
            System.out.println("[Queue] " + queueName + " disk data filter end"
                    + " t:[" + result.get(0).getT() + "," + result.get(result.size() - 1).getT() + "]"
                    + " a:[" + result.get(0).getA() + "," + result.get(result.size() - 1).getA() + "]");
        else
            System.out.println("[Queue] " + queueName + " disk data filter end" + " null");
        result.addAll(restData);
        queueLock.unlock();
        return result;
    }

}
