package io.openmessaging.utils;

import io.openmessaging.object.Block;

import java.util.ArrayList;

public class SearchUtil {

    public static int binSearch(ArrayList<Block> blocks, int size, long tKey) {
        int mid = 0;
        int start = 0;
        int end = size - 1;
        while (start <= end) {
            mid = (end + start) / 2 ;

            if(mid == 0){
                if(tKey <=  blocks.get(mid).getTmin()){
                    end = mid - 1;
                }
                else if(tKey > blocks.get(mid).getTmax()){
                    start = mid + 1;
                }
                else
                    return mid;
            }
            else {
                if (tKey <= blocks.get(mid - 1).getTmax()) {
                    end = mid - 1;
                } else if (tKey > blocks.get(mid).getTmax()) {
                    start = mid + 1;
                } else
                    return mid;
            }
        }
        if(mid == 0)
            return -1;
        else
            return -2;
    }
}
