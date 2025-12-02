package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.sun.org.apache.bcel.internal.generic.NEW;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import javafx.util.Builder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.protocol.Message;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author Felix
 * @date 2024/5/27
 * DIM维度层的处理
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、HBase、DimApp
 * 开发流程
 *      基本环境准备
 *      检查点相关的设置
 *      从kafka主题中读取数据
 *      对流中数据进行类型转换并ETL     jsonStr->jsonObj
 *      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *      使用FlinkCDC读取配置表中的配置信息
 *      对读取到的配置流数据进行类型转换    jsonStr->实体类对象
 *      根据当前配置信息到HBase中执行建表或者删除操作
 *          op=d        删表
 *          op=c、r     建表
 *          op=u        先删表，再建表
 *      对配置流数据进行广播---broadcast
 *      关联主流业务数据以及广播流配置数据---connect
 *      对关联后的数据进行处理---process
 *          new TableProcessFunction extends BroadcastProcessFunction{
 *              open:将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
 *              processElement:对主流数据的处理
 *                  获取操作的表的表名
 *                  根据表名到广播状态中以及configMap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
 *                      Tuple2<dataJsonObj,配置对象>
 *                  在向下游发送数据前，过滤掉了不需要传递的属性
 *                  在向下游发送数据前，补充操作类型
 *              processBroadcastElement:对广播流数据进行处理
 *                  op =d  将配置信息从广播状态以及configMap中删除掉
 *                  op!=d  将配置信息放到广播状态以及configMap中
 *          }
 *     将流中数据同步到HBase中
 *          class HBaseSinkFunction extends RichSinkFunction{
 *              invoke:
 *                  type="delete" : 从HBase表中删除数据
 *                  type!="delete" :向HBase表中put数据
 *          }
 *     优化：抽取FlinkSourceUtil工具类
 *          抽取TableProcessFunction以及HBaseSinkFunction函数处理
 *          抽取方法
 *          抽取基类---模板方法设计模式
 * 执行流程（以修改了品牌维度表中的一条数据为例）
 *      当程序启动的时候，会将配置表中的配置信息加载到configMap以及广播状态中
 *      修改品牌维度
 *      binlog会将修改操作记录下来
 *      maxwell会从binlog中获取修改的信息，并封装为json格式字符串发送到kafka的topic_db主题中
 *      DimApp应用程序会从topic_db主题中读取数据并对其进行处理
 *      根据当前处理的数据的表名判断是否为维度
 *      如果是维度的话，将维度数据传递到下游
 *      将维度数据同步到HBase中
 *
 */
public class DimApp extends BaseApp{
    public static void main(String[] args) throws Exception {
        new DimApp().start(10002,4,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对业务流中数据类型进行转换并进行简单的ETL    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //TODO 使用FlinkCDC读取配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        //TODO 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS = createHBaseTable(tpDS);

        //tpDS.print();
        //TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(tpDS, jsonObjDS);

        //TODO 将维度数据同步到HBase表中
        //({"tm_name":"Redmi","id":1,"type":"update"},TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=r))
        dimDS.print();
        writeToHBase(dimDS);
    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //将配置流中的配置信息进行广播---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //处理关联后的数据(判断是否为维度)
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

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
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //5.1 创建MySQLSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dim");
        //5.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        //"op":"r": {"before":null,"after":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1716812196180,"transaction":null}
        //"op":"c": {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812267000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423611,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1716812265698,"transaction":null}
        //"op":"u": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"aa"},"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812311000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11423960,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1716812310215,"transaction":null}
        //"op":"d": {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"aa"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716812341000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11424323,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1716812340475,"transaction":null}


        //mysqlStrDS.print();

        //TODO 6.对配置流中的数据类型进行转换  jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if("d".equals(op)){
                            //对配置表进行了一次删除操作   从before属性中获取删除前的配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            //对配置表进行了读取、添加、修改操作   从after属性中获取最新的配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");

                        if ("gmall2024".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        //jsonObjDS.print();
        return jsonObjDS;
    }}



    /*public static void main(String[] args) throws Exception {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    // 过滤掉不需要的字段
    private static void deleteNoNeedColumns(JSONObject data, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        // 使用迭代器删除
        *//*Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
        for(;iterator.hasNext();){
            Map.Entry<String, Object> next = iterator.next();
            if(!columnList.contains(next.getKey())){
                iterator.remove();
            }
        }*//*
        entrySet.removeIf(entry -> columnList.contains(entry.getKey()));

    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对业务数据的数据类型进行转换和简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        //TODO 使用flinkCDC读取配置文件中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = resdTableProcess(env);

        //TODO 根据配置信息到Hbase中创建或者删除维度表

        tpDS = createHBaseTable(tpDS);
        //TODO 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);

        //TODO 将维度数据同步到Hbase表中

        writeToHBase(dimDS);
    }

    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        //TODO 8.将配置流中的数据进行广播---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("maoStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 9.将主流的业务数据和广播流中的配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //TODO 10.处理关联后的数据（判断是否为维度数据）
        //10.1 processElement:处理主流业务数据                   根据表名到广播状态中读取配置信息，判断是否为维度
        //10.2 processBroadcastElement:处理广播流配置信息数据      将配置信息数据放到广播状态中  k:维度表名  v:一个配置对象
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHbaseConnection(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                // 获取对维度表的操作类型
                String op = tableProcessDim.getOp();
                // 获取对HBase进行操作的维度表表名
                String sinkTable = tableProcessDim.getSinkTable();
                // 获取在HBase中创建表所需要的列族
                String[] sinkFamilies = tableProcessDim.getSinkFamily().split(",");
                if("d".equals(op)){
                    // 从配置表中删除了一条数据，需要在HBase中将对应的维度表删除
                    HBaseUtil.dropHbaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                }else if ("r".equals(op) || "c".equals(op)){
                    // 从配置表中读取或者添加了一条数据，需要在HBase中将对应的维度表创建出来
                    HBaseUtil.createHbaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                }else {
                    // 从配置表中修改了一条数据，需要在HBase中将对应的维度表进行修改,操作为先删除再创建
                    HBaseUtil.dropHbaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHbaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                }
                return tableProcessDim;
            }
        }).setParallelism(1);
        return tpDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> resdTableProcess(StreamExecutionEnvironment env) {
        //5.1 创建MysqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "gmall2024_config.table_process_dim");
        //5.2 消费数据，封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        //TODO 6.对配置流的配置信息数据的类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    // 对维度表进行删除操作，那么就应该读取json中before的配置信息
                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                } else {
                    // 对配置表进行增删改的操作，应该读取json中after的配置信息
                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
        return tpDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String db = jsonObj.getString("database");
                String type = jsonObj.getString("type");
                String data = jsonObj.getString("data");

                if ("gmall2024".equals(db)
                        && ("insert".equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "bootstrap-insert".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    out.collect(jsonObj);
                }
            }
        });
        return jsonObjDS;
    }
}*/