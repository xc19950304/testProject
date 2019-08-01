package io.openmessaging;

public class QueueCodeUtil {
    public static int getCodeByThreadName(final String threadName) {
        int queueCode = 0;
        for (int i = 7, end = threadName.length(); i < end; i++) {
            queueCode = queueCode * 10 + threadName.charAt(i) - 48;
        }
        return queueCode;
    }
}
