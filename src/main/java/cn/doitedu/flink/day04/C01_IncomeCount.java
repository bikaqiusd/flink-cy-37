package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;


/**
 * 山东省,济南市,2000
 * 山东省,烟台市,1000
 * 山东省,济南市,3000
 * 山东省,淄博市,2000
 * 辽宁省,大连市,2000
 * 辽宁省,沈阳市,3000
 *
 * 实时输入数据，使用统计每个城市的成交金额，并且再统计出每个省份的成交金额
 * 山东省,2000,济南市,2000
 * 山东省,3000,烟台市,1000
 * 山东省,5000,淄博市,2000
 */
public class C01_IncomeCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");

                return Tuple3.of(fields[1], fields[2], Double.parseDouble(fields[3]));
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple4<String, Double, String, Double>> res = keyedStream.map(new ProvinceAndCityCountFunction());

        res.print();

        env.execute();
    }

    public static class ProvinceAndCityCountFunction implements MapFunction<Tuple3<String, String, Double>, Tuple4<String, Double,String, Double>>{
        private HashMap<String,HashMap<String,Double>> data = new HashMap<>();

        @Override
        public Tuple4<String, Double, String, Double> map(Tuple3<String, String, Double> in) throws Exception {

            String province = in.f0;
            HashMap<String, Double> cityToMoney = data.get(province);
            if(cityToMoney == null){
                cityToMoney = new HashMap<>();
            }

            String city = in.f1;
            Double money = cityToMoney.get(city);
            if(money == null){
                money = 0.0;
            }

            money += in.f2;
            cityToMoney.put(city,money);
            data.put(province,cityToMoney);
            double total = 0.0;

            for(Double value: cityToMoney.values()){
                total += value;
            }

            return Tuple4.of(province,total,city,money);


        }
    }
}
