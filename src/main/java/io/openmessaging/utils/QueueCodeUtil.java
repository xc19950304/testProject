package io.openmessaging.utils;

import io.openmessaging.Constants;

public class QueueCodeUtil {
    //7 12
    public static int getCodeByThreadName(final String threadName) {
        int queueCode = 0;
        for (int i = Constants.THREAD_CODE_NUMBER, end = threadName.length(); i < end; i++) {
            queueCode = queueCode * 10 + threadName.charAt(i) - 48;
        }
        return queueCode;
    }
}
