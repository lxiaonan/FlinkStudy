package com.study.api1.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 断点式水位线生成器
 * @param <T>
 */
public class WatermarkBreakPointCustom<T> implements WatermarkGenerator<T> {
    private long dailyTs;
    private long maxTs;

    public WatermarkBreakPointCustom(long dailyTs) {
        this.dailyTs = dailyTs;
        maxTs = Long.MIN_VALUE + dailyTs + 1;
    }

    /**
     * 每来一条数据，都会调用这个方法
     * 断点式水位线，当有数据来的时候，才会去生成水印
     * @param event
     * @param eventTimestamp 数据的时间戳
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - dailyTs - 1));
        System.out.println("当前事件时间：" + eventTimestamp + ", 最大时间" + maxTs);
    }

    /**
     * 周期性调用，不做操作
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}
