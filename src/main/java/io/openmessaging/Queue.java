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
    public final static int FLUSH_MESSAGE_NUMBER = MESSAGE_NUMBER * 13;//65000(50*1300)

    //每个读写缓冲区的延迟消息个数
    public final static int DELAY_NUMBER = MESSAGE_NUMBER * 10;//非常重要，用于保证每个块间的延迟性;

    //当前Queue对应的块个数，每个块64k-1310条消息,有BLOCK_SIZE个块
    public final static int BLOCK_SIZE = 50000;

    private static Comparator<Message> comparator = (o1, o2) -> (int) (o1.getT() - o2.getT());

    //写缓冲区
    private   ByteBuffer writeBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * MESSAGE_NUMBER);

    private   ByteBuffer readBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);

    //private  final static ByteBuffer readAndWriteBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * (MESSAGE_NUMBER + DELAY_NUMBER));


    private java.util.Queue<Message> messageBuffer = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);

    //消息个数
   // private AtomicInteger currentMessageNumber = new AtomicInteger(0);

    //64k刷一次盘，ISOP的最优刷盘值
    private ByteBuffer flushBuffer = ByteBuffer.allocateDirect(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);//  约等于64k

    //记录当前操作的是哪个块
    private int blockIndex = -1;

    //记录当前是否是首次操作该块
    private boolean firstPut = true;

    private FileChannel channel;

    //记录当前操作块的物理偏移
    private AtomicLong writePosition;


/*    // 记录块每个块中首消息的t值
    private long blockTMin[] = new long[BLOCK_SIZE];

    // 记录块每个块中末尾消息的t值
    private long blockTMax[] = new long[BLOCK_SIZE];

    // 记录每个块在物理文件中的起始偏移量
    private long offsets[] = new long[BLOCK_SIZE];*/


    private ArrayList<BlockInfo> blocks = new ArrayList<BlockInfo>();

    private BlockInfo currentBlock;

    private Future<Long> flushFuture;

    private static ExecutorService flushThread = Executors.newSingleThreadExecutor();

    private String queueName;

    Queue(FileChannel channel, AtomicLong writePosition, String queueName){
        this.channel = channel;
        this.writePosition = writePosition;
        this.queueName = queueName;
        //offsets[0] = writePosition.get();
    }

    //对当前message进行block内的排序
/*    private void sortAndPutToBlock(Message message){

       // readAndWriteBuffer.put(message);
    }

    public void put(Message message) {
        // 缓冲区满，先落盘
        if (readAndWriteBuffer.remaining() < MESSAGE_SIZE) {
            // 落盘
            flush();
            blockIndex++;
            //firstPut = true;
        }
        //对当前message和当前buffer里的数据排序并入队
        sortAndPutToBlock(message);
    }*/

/*    private void flush() {
        //将这个已经快20条数据的buffer刷到64k的buffer中，做异步flush操作
        flushFuture = flushThread.submit(() -> {
            long writePosition;
            try {
                if (flushBuffer.remaining() < MESSAGE_SIZE * MESSAGE_NUMBER) {
                    flushBuffer.flip();
                    writePosition = this.writePosition.get();//获取刷块时的物理地址
                    channel.write(flushBuffer);
                    flushBuffer.clear();
                }

                readAndWriteBuffer.flip();
                byte[] writeByte = new byte[MESSAGE_SIZE * MESSAGE_NUMBER];
                byte[] delayByte = new byte[MESSAGE_SIZE * DELAY_NUMBER];

                readAndWriteBuffer.get(writeByte);
                flushBuffer.put(writeByte);

                readAndWriteBuffer.get(delayByte);
                readAndWriteBuffer.clear();
                readAndWriteBuffer.put(delayByte);

            } catch (IOException e) {
                e.printStackTrace();
            }
            return writePosition;
        });
        //起异步任务获取上一个块的偏移量
        if (flushFuture != null) {
            try {
                offsets[blockIndex] = flushFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            flushFuture = null;
        }
    }*/
    private boolean thisBlockFisrtPut = true;
    //有个问题就是没办法处理最后没有不满30条的消息，该消息一直在优先队列中
    //同一时刻一个Queue的put和flush串行执行
    public void put(Message message) {
        queueLock.lock();
        if(thisBlockFisrtPut){
            //blockIndex++;
            currentBlock = new BlockInfo();
            currentBlock.setTmin(segmentStartT);
            currentBlock.setQueueName(queueName);
            blocks.add(currentBlock);
            //blockTMin[++blockIndex] = segmentStartT;
            thisBlockFisrtPut = false;
            System.out.println(queueName + " block " + (blocks.size()) + " put begin" );
        }
        if (messageBuffer.size() == (MESSAGE_NUMBER + DELAY_NUMBER)) {
/*            if(queueName.equals("queue1")) {
                System.out.println(queueName + ":----put end----");
                System.out.println(queueName + ":-flush begin--buffer_queue_size:" + messageBuffer.size());
            }*/
            flush();
/*            if(queueName.equals("queue1")) {
                System.out.println(queueName + ":----put start----");
                System.out.println(queueName + ":-flush end--buffer_queue_size:"+messageBuffer.size());
            }*/
        }
/*        if(queueName.equals("queue1"))
            System.out.println(queueName+ "-message_T:" + message.getT());*/
        messageBuffer.add(message);

        queueLock.unlock();
    }


    private long segmentStartT = 0;
    private long segmentEndT = 0;
    //private boolean firstFlush = true;
    //将队列中20条数据刷到64k的buffer中，做异步flush操作
    private void flush() {
/*        if(queueName.equals("queue1")) {
            System.out.println(queueName + ":----flush start-----");
            System.out.println(queueName + "-buffer_remaining:" + writeBuffer.remaining());
        }*/
        for (int i = 0; i < MESSAGE_NUMBER; i++) {
            Message message = messageBuffer.poll();
/*            if(queueName.equals("queue1"))
                System.out.println(queueName + "-message_T:" + message.getT());
            if(blocks.size() == 115)
                System.out.println(message.getT() + "----" + message.getA());*/
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
/*        if(queueName.equals("queue1")) {
            System.out.println(queueName + ":----flush end-----");
            System.out.println(queueName + "-buffer_remaining:" + writeBuffer.remaining());
        }*/

        flushFuture = flushThread.submit(() -> {
            long writePosition = -1L;
            try {
                if (flushBuffer.remaining() < MESSAGE_SIZE * MESSAGE_NUMBER) {
                    System.out.println(queueName + " block " + (blocks.size()) + " put finished  and  tmin:" + currentBlock.getTmin() + " tmax:" + currentBlock.getTmax()   );

                    flushBuffer.flip();
                    writePosition = this.writePosition.get();//获取刷块时的物理地址
                    channel.write(flushBuffer);
                    this.writePosition.getAndAdd(MESSAGE_SIZE * FLUSH_MESSAGE_NUMBER);
                    flushBuffer.clear();
                    //System.out.println(" tmin：" + currentBlock.getTmin() + " tmax：" + currentBlock.getTmax() );

                    //刷盘更新下一个block初始化参数
                    thisBlockFisrtPut = true;
/*                    blockIndex++;
                    blockTMin[blockIndex] = segmentStartT;*/

                    //System.out.println("----------flush To Dist------------blockIndex:"+blockIndex + queueName);
                    //System.out.println("----------flush To Dist------------writePosition:"+writePosition+ queueName);
                }
                else
                    currentBlock.setTmax(segmentEndT);

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

    public synchronized void getAll(long aMin, long aMax, long tMin, long tMax){
        //System.out.println(queueName);
        int size = blocks.size();

        //最后一个block不一定刷盘，且数据存在优先队列(必有)和flush_buffer(可能有)中，单独考虑
        //处理flush_buffer
        int messageNum = flushBuffer.remaining() / MESSAGE_SIZE;
        if (messageNum != 0) {
            size = size - 1;
        }


        //共有多少个block
        for (int j = 0; j <size; j++) {
            readBuffer.clear();
            try {
                channel.read(readBuffer, blocks.get(j).getStartOffset());
            } catch (IOException e) {
                e.printStackTrace();
            }
            readBuffer.flip();
            for (int i = 0; i < FLUSH_MESSAGE_NUMBER; i++) {
                byte[] body = new byte[MESSAGE_SIZE-8-8];
                long t = readBuffer.getLong();
                long a = readBuffer.getLong();
                readBuffer.get(body);
                Message msg = new Message(a,t,body);

               //System.out.println("blockId:"+ j + " " +blocks.get(j).getTmin()+ "-"+blocks.get(j).getTmax() +  ",t:" + t );

            }
        }
        //System.out.println("--------------------flush buffer data-----------------");

        flushBuffer.flip();
        if (messageNum != 0) {
            for (int i = 0; i < messageNum; i++) {
                byte[] body = new byte[MESSAGE_SIZE - 8 - 8];
                long t = flushBuffer.getLong();
                long a = flushBuffer.getLong();
                flushBuffer.get(body);
                Message msg = new Message(a, t, body);
                //System.out.println("t:" + t );
            }
        }


        //System.out.println("--------------------message buffer data-----------------");
        java.util.Queue<Message> tempQueue = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);
        while (!messageBuffer.isEmpty()) {
            Message msg = messageBuffer.poll();
            tempQueue.offer(msg);
            /*long a = msg.getA();
            long t = msg.getT();
            System.out.println("t:" + t );*/
        }
        messageBuffer = tempQueue;
    }

    Lock queueLock = new ReentrantLock();
    //同一时刻只有
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {

/*        aMin = 438175;
        aMax = 538175;
        tMin = 538171;
        tMax = 633107;*/
        queueLock.lock();
        //System.out.println("[Queue] " + queueName + " getMessage begin ");
        List<Message> result = new ArrayList<>();
        List<Message> restData = new ArrayList<>();
        int size = blocks.size();

        //最后一个block不一定刷盘，且数据存在优先队列(必有)和flush_buffer(可能有)中，单独考虑
        //处理flush_buffer
        System.out.println("[Queue] " + queueName +" get flushBuffer rest data");
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
        System.out.println("[Queue] " + queueName +" the length of flushBuffer is " + restData.size());

        //处理queue_buffer
        System.out.println("[Queue] " + queueName +" get priorityQueue data rest data");
        java.util.Queue<Message> tempQueue = new PriorityBlockingQueue<Message>(MESSAGE_NUMBER + DELAY_NUMBER, comparator);
        while (!messageBuffer.isEmpty()) {
            Message msg = messageBuffer.poll();
            tempQueue.offer(msg);
            long a = msg.getA();
            long t = msg.getT();
            if (t >= tMin && t <= tMax && a >= aMin && a <= aMax)
                restData.add(msg);
        }
        messageBuffer = tempQueue;
        System.out.println("[Queue] " + queueName +" the length of priorityQueue and flushBuffer is " + restData.size());

        //处理已刷盘数据
        System.out.println("[Queue] " + queueName +" get ssd disk data rest data");
        int startBlock = size-1;
        int endBlock = 0;
        for (int i = 0; i < size-1; i++) {
            if (i == 0 && tMin <= blocks.get(i).getTmax()) {
                startBlock = i;
                break;
            } else if (tMin > blocks.get(i).getTmax() && tMin <= blocks.get(i + 1).getTmax()) {
                startBlock = i + 1;
                break;
            }
        }
        for (int i = size - 1; i > 0; i--) {
            if (i == size - 1 && tMax >= blocks.get(i).getTmin()) {
                endBlock = i;
                break;
            } else if (tMax < blocks.get(i).getTmin() && tMax >= blocks.get(i-1).getTmin()) {
                endBlock = i-1;
                break;
            }
        }
        for (int j = startBlock; j <= endBlock; j++) {
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
                Message msg = new Message(a, t, body);
                if (j == startBlock && t < tMin || j == endBlock && t > tMax)
                    continue;
                if (a >= aMin && a <= aMax)
                    result.add(msg);
                }
    }
        System.out.println("[Queue] " + queueName +" ,the length of ssd data is " + result.size());
        result.addAll(restData);
        System.out.println("[Queue] " + queueName +" ,the length of this queue data is " + result.size());
        queueLock.unlock();
        //System.out.println("[Queue] " + queueName + " getMessage finished ");
        /*  for(Message m: result){
            System.out.println(m.getT());
        }*/
        return result;

    }

}
