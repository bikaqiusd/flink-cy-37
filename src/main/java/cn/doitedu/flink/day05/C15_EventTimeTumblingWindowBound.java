package cn.doitedu.flink.day05;

/**
 * EventTime滚动窗口的时间范围边界
 *
 * 1.flink中的时间窗口时间精确到毫秒
 * 2.flink中的窗口的起始时间，结束时间是对齐的（即窗口的起始时间，结束时间是窗口长度的整数倍）
 * 3.flink中的窗口范围是前闭后开的范围[5000, 10000) 或 [5000,9999]
 */
public class C15_EventTimeTumblingWindowBound {

    public static void main(String[] args) {

        //long eventTime = 10002345;

        long eventTime = 1000;
        long windowSize = 5000;

        long winStart = eventTime - eventTime % windowSize;
        long winEnd = winStart + windowSize;

        System.out.println(eventTime  + " 对应的窗口时间范围是 ：[" + winStart + "," + winEnd + ")");

    }


}
