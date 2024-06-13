package cn.doitedu.flink.day06;

/**
 * EventTime滑动窗口的时间范围边界
 *
 * 1.flink中的时间窗口时间精确到毫秒
 * 2.flink中的滑动窗口的起始时间，结束时间是对齐的（即窗口的起始时间，结束时间是窗口滑动步长的整数倍）
 * 3.flink中的窗口范围是前闭后开的范围[5000, 10000) 或 [5000,9999]
 */
public class C03_EventTimeSlidingWindowBound {

    public static void main(String[] args) {

        //long eventTime = 10002345;

        long eventTime = 6666;
        long windowSize = 10000; //窗户的长度
        long slideSize = 5000; //滑动步长


        long winStart = eventTime - eventTime % slideSize - slideSize;
        long winEnd = winStart + windowSize;

        System.out.println(eventTime  + " 对应的滑动窗口时间范围是 ：[" + winStart + "," + winEnd + ")");

    }


}
