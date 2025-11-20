package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/6/02
 * 评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp{
    /*public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度
        env.setParallelism(4);
        //1.3创建表处理环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);*/

        //TODO 2.检查点相关的设置
        //*/2.1开启检查点
        /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //2.3设置状态取消后，检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4设置两个检查点之间最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        //2.6设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        //2.7设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/

        //TODO 3.从kafka的topic_DB主题中读取业务数据      ---kafka连接器
        /*tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");*/

        //TODO 4.从业务数据中过滤出评论数据 并生成动态表     ---where  table = 'comment_info'    type = 'insert'
        /*Table commentInfo = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='comment_info' and `type`='insert'");*/

        //将评论表注册到表执行环境中
        /*tableEnv.createTemporaryView("comment_info", commentInfo);*/

        //TODO 5.从Hbase表中读取字典数据 并生成动态表      ---Hbase连接器
        /*tableEnv.executeSql("CREATE TABLE hTable (\n" +
                " rowkey INT,\n" +
                " family1 ROW<q1 INT>,\n" +
                " family2 ROW<q2 STRING, q3 BIGINT>,\n" +
                " family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'mytable',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181'\n" +
                ");");*/

        //TODO 6.使用lookup-join关联评论数据和字典数据    ---lookup join
        /*Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "    ON c.appraise = dic.dic_code");*/

        //TODO 7.将关联的结果写入到kafka的主题中          ---upsert kafka连接器
        //7.1创建动态表和要写入的kafka主题进行映射
        /*tableEnv.executeSql("CREATE TABLE pageviews_per_region (\n" +
                "  user_region STRING,\n" +
                "  pv BIGINT,\n" +
                "  uv BIGINT,\n" +
                "  PRIMARY KEY (user_region) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'pageviews_per_region',\n" +
                "  'properties.bootstrap.servers' = '...',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        //7.2写入
        joinedTable.executeInsert("pageviews_per_region");*/

















    public static void main(String[] args) {
         new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db主题中读取数据 创建动态表       ---kafka连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 过滤出评论数据                                ---where table='comment_info'  type='insert'
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db where `table`='comment_info' and `type`='insert'");
        //commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);

        //TODO 从HBase中读取字典数据 创建动态表                ---hbase连接器
        readBaseDic(tableEnv);

        //TODO 将评论表和字典表进行关联                        --- lookup Join
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "    ON c.appraise = dic.dic_code");
        //joinedTable.execute().print();

        //TODO 将关联的结果写到kafka主题中                    ---upsert kafka连接器
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }



}
