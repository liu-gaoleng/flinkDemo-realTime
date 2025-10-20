package learno.app;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 *
 * 下单事实表
 */
public class DwdOricalDetile extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdOricalDetile().start(10014, 4, Constant.TOPIC_DWD_ORICAL_DETILE)
    }
    @Override
    public void Handle(StreamTableEnvironment){
        // TODO 设置状态保留时间【传输延迟 + 业务上的滞后关系】
        tableEnv.getconfig().setIdlestateRetetion(Duration.ofSeconds(10));
        // TODO 从kafka的topic中读取数据并创建表

        // TODO 过滤出订单明细数据
        // TODO 过滤出订单数据
        // TODO 过滤出明细活动数据
        // TODO 过滤出明细优惠券数据
        // TODO 关联上述四张表
        // TODO 将关联的结果写入到kafka主题中
        // 创建动态表和要写入的主题进行映射
        // 写入
    }

}
