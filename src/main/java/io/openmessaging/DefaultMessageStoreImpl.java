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

    private Map<String, Queue> queueMaps = new HashMap();

    private int queueSize = 10;

    private FileChannel[] channels = new FileChannel[queueSize];

    private AtomicLong[] writePosition = new AtomicLong[queueSize];

    //private String dir = "/Users/xiongchang.xc/race2019/";

    private String dir = "/alidata1/race2019/data/";

    private Random rand = new Random();

    private static Comparator<Message> comparator = (o1, o2) -> (int) (o1.getT() - o2.getT());

    DefaultMessageStoreImpl() {
        for(int i = 0;i<queueSize;i++) {
            RandomAccessFile memoryMappedFile = null;
            try {
                memoryMappedFile = new RandomAccessFile(dir + "queue"+ i + ".data", "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            channels[i] = memoryMappedFile.getChannel();
            writePosition[i] = new AtomicLong(0);
        }
    }



    @Override
    public void put(Message message) {
        int queueNumber = Math.abs(Thread.currentThread().getName().hashCode()) % queueSize;
        String queueName = "queue" + queueNumber;
        Queue queue = queueMaps.get(queueName);
        if (queue == null) {
            synchronized (this) {
                queue = queueMaps.get(queueName);
                if (queue == null) {
                    queue = new Queue(channels[queueNumber], writePosition[queueNumber], queueName);
                    queueMaps.put(queueName, queue);
                }
            }
        }
        queue.put(message);
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        List<Message> res = new ArrayList<>();
/*        List<Integer> queueRandRange = new ArrayList<>();
        for(int i=0;i<queueSize;i++){
            queueRandRange.add(i);
        }
        for (int i = 0; i < queueSize; i++) {
            int queueNum = rand.nextInt(queueRandRange.size());
            String queueName = "queue" + queueRandRange.get(queueNum);
            //System.out.println("[DefaultMessageStoreImpl]"+ Thread.currentThread().getName() + " " +queueName);
            queueRandRange.remove(queueNum);
            List<Message> messageList = queueMaps.get(queueName).getMessage(aMin, aMax, tMin, tMax);
            res.addAll(messageList);
        }
        Collections.sort(res, comparator);
        for(int i=0;i<queueSize;i++){
            queueRandRange.add(i);
        }*/
        /*aMin = 554702;aMax = 554802;tMin = 639549;tMax = 640016;*/
        //System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " request begin");
        for (int i = 0; i < queueSize; i++) {
            String queueName = "queue" + i;
            //System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " " + queueName + " getMessage begin ");
            List<Message> messageList = queueMaps.get(queueName).getMessage(aMin, aMax, tMin, tMax);
            //System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " " + queueName + " getMessage finished ");
            res.addAll(messageList);
        }
        Collections.sort(res, comparator);
        //System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " " + "request end");
        //System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " " + " aMin = [" + aMin + "], aMax = [" + aMax + "], tMin = [" + tMin + "], tMax = [" + tMax + "]");
/*
        if (res != null && res.size() > 0)
            System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " min = [" + res.get(0).getT() + "], max = [" + res.get(res.size() - 1).getT() + "]");
        else
            System.out.println("[DefaultMessageStoreImpl] " + Thread.currentThread().getName() + " result is null");
*/

        return res;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        long count = 0;
        for (int i = 0; i < queueSize; i++) {
            String queueName = "queue" + i;
            List<Message> messageList = queueMaps.get(queueName).getMessage(aMin, aMax, tMin, tMax);
            int size = messageList.size();
            for (int j = 0; j < size; j++) {
                sum += messageList.get(j).getA();
            }
            count += size;
        }
        return count == 0 ? 0 : sum / count;
    }

}
