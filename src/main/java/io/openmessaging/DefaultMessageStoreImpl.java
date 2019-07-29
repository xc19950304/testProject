package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    private NavigableMap<Long, List<Message>> msgMap = new TreeMap<>();

    private Map<String, Queue> queueMaps  = new HashMap();

    private int queueSize  = 10;

    private FileChannel channels;

    private AtomicLong writePosition = new AtomicLong(0L);

    //private String dir = "/Users/xiongchang.xc/race2019/";
    private String dir = "/alidata1/race2019/data/";

    private Random rand = new Random();

    private static Comparator<Message> comparator = (o1, o2) -> (int) (o1.getT() - o2.getT());

    DefaultMessageStoreImpl()
    {
        RandomAccessFile memoryMappedFile = null;
        try {
            memoryMappedFile = new RandomAccessFile(dir + "all"  + ".data", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        channels = memoryMappedFile.getChannel();
    }


    @Override
    public void put(Message message) {
        int queueNumber = rand.nextInt(100)% queueSize;
        String queueName = "queue" + queueNumber;
        Queue queue = queueMaps.get(queueName);
        if (queue == null) {
            synchronized (this) {
                queue = queueMaps.get(queueName);
                if (queue == null) {
                    queue = new Queue(channels, writePosition, queueName);
                    queueMaps.put(queueName, queue);
                }
            }
        }
        queue.put(message);
    }


    @Override
    public synchronized List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> res = new ArrayList<Message>();
        for (int i = 0; i < queueSize; i++){
            List<Message> messageList = new ArrayList<Message>();
            String queueName = "queue" + i;
            messageList = queueMaps.get(queueName).getMessage(aMin, aMax, tMin, tMax);
            res.addAll(messageList);
        }
        Collections.sort(res,comparator);

/*
        System.out.println(Thread.currentThread().getName() + "aMin = [" + aMin + "], aMax = [" + aMax + "], tMin = [" + tMin + "], tMax = [" + tMax + "]");
        if(res != null)
            System.out.println(Thread.currentThread().getName() + "min = [" + res.get(0).getT() +  "], max = [" + res.get(res.size()-1).getT() + "]");
*/

        return res;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        long count = 0;
        for (int i = 0; i < queueSize; i++){
            String queueName = "queue" + i;
            List<Message> messageList = queueMaps.get(queueName).getMessage(aMin, aMax, tMin, tMax);
            int size = messageList.size();
            for(int j = 0; j < size; j++){
                sum += messageList.get(j).getA();
            }
            count += size;
        }
        return sum / count;
    }

}
