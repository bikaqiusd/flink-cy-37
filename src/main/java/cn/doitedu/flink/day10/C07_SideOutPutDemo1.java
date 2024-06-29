package cn.doitedu.flink.day10;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 该例子演示侧流输出，就是将数据流中的数据按照类型，打上不同的标签
 * 然后根据标签类型，获取指定类型的数据
 */
public class C07_SideOutPutDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1
        //2
        //a
        //3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        OutputTag<Integer> oddTag = new OutputTag<Integer>("oddTag") {
        };
        OutputTag<Integer> evenTag = new OutputTag<Integer>("evenTag") {
        };
        OutputTag<String> strTag = new OutputTag<String>("strTag") {
        };



//        SingleOutputStreamOperator<String> mainStream2 = lines.process((value,ctx,out)->{
//
//            try {
//                int i = Integer.parseInt(value);
//                if (i % 2 != 0) {
//                    //奇数
//                    ctx.output(oddTag, i); //将数据和类型绑定在一起输出
//                } else {
//                    //偶数
//                    ctx.output(evenTag, i);
//                }
//            } catch (NumberFormatException e) {
//                //e.printStackTrace();
//                //字符串
//                ctx.output(strTag, value);
//            }
//
//            //输出未打标签的数据（正常的）
//            out.collect(value);
//
//        });

        SingleOutputStreamOperator<String> mainStream1 = lines.process(new ProcessFunction<String, String>() {

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 != 0) {
                        //奇数
                        ctx.output(oddTag, i); //将数据和类型绑定在一起输出
                    } else {
                        //偶数
                        ctx.output(evenTag, i);
                    }
                } catch (NumberFormatException e) {
                    //e.printStackTrace();
                    //字符串
                    ctx.output(strTag, value);
                }

                //输出未打标签的数据（正常的）
                out.collect(value);
            }
        });


        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 != 0) {
                        ctx.output(oddTag, i);
                    } else {
                        ctx.output(evenTag, i);
                    }
                } catch (NumberFormatException e) {
                    ctx.output(strTag, value);
                }

                out.collect(value);

            }
        });



        DataStream<Integer> eventStreeam = mainStream.getSideOutput(evenTag);

        DataStream<String> Streeam = mainStream.getSideOutput(strTag);

        mainStream.print();

        env.execute();


    }
}
