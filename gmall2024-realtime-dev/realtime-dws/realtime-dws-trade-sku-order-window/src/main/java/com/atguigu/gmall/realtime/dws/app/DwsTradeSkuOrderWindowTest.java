package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DimMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import com.sun.crypto.provider.OAEPParameters;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.codehaus.stax2.validation.Validatable;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DwsTradeSkuOrderWindowTest extends BaseApp {



    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindowTest().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }





    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 过滤空消息 对流中的数据类型进行转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                if (jsonStr != null) {
                    collector.collect(JSON.parseObject(jsonStr));
                }
            }
        });

        //TODO 按照唯一键（订单明细的id）进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));

        //TODO 去重
        //去重方式1：状态 + 定时器
        /*SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastJsonObjState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                //从状态中获取上次接收到的json对象
                JSONObject lastJsonObj = lastJsonObjState.value();
                if (lastJsonObj == null) {
                    //说明当前没有重复 将接收到的这条json放到状态中 并注册5s后执行的定时器
                    lastJsonObjState.update(jsonObject);
                    long currentProcessingTime = context.timerService().currentProcessingTime();
                    context.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);

                } else {
                    //说明当前重复了 用当前json的聚合时间和状态中的json的聚合时间进行比较 将时间大的json更新到状态中
                    String lastTS = lastJsonObj.getString("聚合时间戳");
                    String curTS = jsonObject.getString("聚合时间戳");
                    if (curTS.compareTo(lastTS) >= 0) {
                        lastJsonObjState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //当定时器被触发执行时 要将状态中的数据传递到下游去 然后将状态中的数据进行清除
                JSONObject jsonObj = lastJsonObjState.value();
                out.collect(jsonObj);
                lastJsonObjState.clear();
            }
        });*/

        //去重方式2：状态 + 抵消
        SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastJsonObjState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                //从状态中获取上次接收到的数据
                JSONObject lastJsonObj = lastJsonObjState.value();
                if (lastJsonObj != null) {
                    //说明重复了，将已经发送到下游的数据（状态），影响到度量值的字段进行取反后再传递到下游
                    String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                    String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                    String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                    String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                    lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                    lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                    lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                    lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                    collector.collect(lastJsonObj);
                }

                lastJsonObjState.update(jsonObject);
                collector.collect(jsonObject);
            }
        });


        //TODO 指定watermark和事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts") * 1000;
                            }
                        })
        );

        //TODO 再次对流中的数据进行类型转换 jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = distinctDS.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                TradeSkuOrderBean tradeSkuOrderBean = new TradeSkuOrderBean();
                String skuId = jsonObj.getString("sku_id");
                BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                Long ts = jsonObj.getLong("ts") * 1000;
                TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                        .skuId(skuId)
                        .originalAmount(splitOriginalAmount)
                        .couponReduceAmount(splitCouponAmount)
                        .activityReduceAmount(splitActivityAmount)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                return tradeSkuOrderBean;
            }
        });

        //TODO 分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(tradeSkuOrderBean -> tradeSkuOrderBean.getSkuId());

        //TODO 开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TradeSkuOrderBean orderBean = iterable.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        collector.collect(orderBean);
                    }
                }
        );

        //TODO 关联sku维度
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String skuId = tradeSkuOrderBean.getSkuId();
                        //根据维度的主键到Hbase维度表中获取对应的维度对象
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        tradeSkuOrderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        tradeSkuOrderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        tradeSkuOrderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        tradeSkuOrderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return tradeSkuOrderBean;
                    }
                }
        );*/
        //优化1：旁路缓存
        /*SingleOutputStreamOperator<TradeSkuOrderBean> witnSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection HbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        HbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(HbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        //先根据流中对象获取要关联的维度的主键
                        String skuId = tradeSkuOrderBean.getSkuId();
                        //根据主键到Redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            //如果在Redis中找到了维度，直接作为查询结果返回
                            System.out.println("~~~从Redis中查询维度数据~~~");
                        } else {
                            //如果在Redis中没找到维度，就到Hbase中查询，并将查询结果缓存到Redis中
                            dimJsonObj = HBaseUtil.getRow(HbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                System.out.println("~~~从HBase中查询维度数据~~~");
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~没有找到要关联的维度~~~");
                            }
                        }
                        //最后将查询到的维度数据补充到流中对象上
                        if(dimJsonObj != null){
                            tradeSkuOrderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }

                        return tradeSkuOrderBean;
                    }
                }
        );*/
        //使用旁路缓存模板关联维度
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSkuId();
                    }
                }
        );*/
        //优化2：异步IO
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean tradeSkuOrderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据当前流中对象获取要关联的主键
                        String skuId = tradeSkuOrderBean.getSkuId();
                        //根据主键到Redis中查找维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_sku_info", skuId);
                        if(dimJsonObj != null){
                            //如果在Redis中找到了维度数据，直接作为结果返回
                            System.out.println("~~~从Redis异步获取维度数据~~~");
                        }else {
                            //如果在Redis中没有找到维度数据，就去HBase中查找维度数据
                            System.out.println("~~~从HBase异步获取维度数据~~~");
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId);
                            //将从HBase中查找到的数据放到Redis中缓存起来，方便下次查询使用
                            if(dimJsonObj != null){
                                RedisUtil.writeDimAsync(redisAsyncConn, "dim_sku_info", skuId, dimJsonObj);
                            }else {
                                System.out.println("~~~未获取到维度数据~~~");
                            }
                        }
                        //将维度对象相关的维度属性补充到流中对象上
                        tradeSkuOrderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        //获取数据库交互的结果并发送给ResultFuture的回调函数，将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(tradeSkuOrderBean));

                    }
                },
                60,
                TimeUnit.SECONDS
        );*/

        //异步IO+模板
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        //TODO 关联spu维度
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = witnSkuInfoDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        //首先获取要关联维度的主键
                        String spuId = tradeSkuOrderBean.getSpuId();
                        //根据主键到Redis中获取维度数据
                        JSONObject dimSpuJsonObj = RedisUtil.readDim(jedis, "dim_spu_info", spuId);
                        if (dimSpuJsonObj != null) {
                            //如果从Redis中获取到了维度数据，直接作为结果返回
                            System.out.println("~~~从Redis中获取维度数据~~~");
                        } else {
                            //如果从Redis中未获取到维度数据，就到HBase中获取维度数据，并将获取到的维度数据放到Redis中缓存起来，方便下次关联使用
                            dimSpuJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_spu_info", spuId, JSONObject.class);
                            if (dimSpuJsonObj != null) {
                                System.out.println("~~~从HBase中获取维度数据~~~");
                                RedisUtil.writeDim(jedis, "dim_spu_info", spuId, dimSpuJsonObj);
                            } else {
                                System.out.println("~~~未获取到维度数据~~~");
                            }
                        }

                        //最终将查询出的维度数据补充到流中的对象上
                        if(dimSpuJsonObj != null){
                            tradeSkuOrderBean.setSpuName(dimSpuJsonObj.getString("spu_name"));
                        }

                        return tradeSkuOrderBean;
                    }
                }
        );*/
        /*SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }


                    @Override
                    public void asyncInvoke(TradeSkuOrderBean tradeSkuOrderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据当前流中对象获取要关联的主键
                        String spuId = tradeSkuOrderBean.getSpuId();
                        //根据主键到Redis中查找维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_spu_info", spuId);
                        if(dimJsonObj != null){
                            //如果在Redis中找到了维度数据，直接作为结果返回
                            System.out.println("~~~从Redis异步获取维度数据~~~");
                        }else {
                            //如果在Redis中没有找到维度数据，就去HBase中查找维度数据
                            System.out.println("~~~从HBase异步获取维度数据~~~");
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, "dim_spu_info", spuId);
                            //将从HBase中查找到的数据放到Redis中缓存起来，方便下次查询使用
                            if(dimJsonObj != null){
                                RedisUtil.writeDimAsync(redisAsyncConn, "dim_spu_info", spuId, dimJsonObj);
                            }else {
                                System.out.println("~~~未获取到维度数据~~~");
                            }
                        }
                        //将维度对象相关的维度属性补充到流中对象上
                        tradeSkuOrderBean.setSpuName(dimJsonObj.getString("spu_name"));
                        //获取数据库交互的结果并发送给ResultFuture的回调函数，将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(tradeSkuOrderBean));


                    }
                },
                60,
                TimeUnit.SECONDS
        );*/

        //异步IO+模板
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setSpuName(dimJsonObj.getString("sku_name"));

                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        //TODO 关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmInfoDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean obj, JSONObject dimJsonObj) {
                        obj.setTrademarkName(dimJsonObj.getString("tm_name"));

                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean obj) {
                        return obj.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 关联category3维度
        //TODO 关联category2维度
        //TODO 关联category1维度

        //TODO 将关联的结果写到Doris表中
        withTmInfoDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));



    }
}

































