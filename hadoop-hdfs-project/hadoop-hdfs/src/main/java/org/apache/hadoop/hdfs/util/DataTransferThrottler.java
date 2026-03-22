// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.util;

import static org.apache.hadoop.util.Time.monotonicNow;

/** 
 * HDFS数据传输流量限制器，用于控制数据传输的带宽速率。
 * 该类是线程安全的，可被多个线程共享，所有线程共享设定的总带宽限制。
 * 在HDFS数据拷贝、块传输等场景中用于限制IO速率，避免占满集群带宽。
 */
public class DataTransferThrottler {
  private final long period;          // 带宽限制的统计周期（毫秒）
  private final long periodExtension; // 允许累计带宽的最大周期范围（毫秒）
  private long bytesPerPeriod;  // 每个周期允许传输的最大字节数
  private long curPeriodStart;  // 当前统计周期的起始时间
  private long curReserve;      // 当前周期剩余可传输的字节数
  private long bytesAlreadyUsed; // 当前周期已经使用的字节数

  /** 
   * 构造函数，使用默认500ms统计周期创建流量限制器
   * @param bandwidthPerSec 允许的每秒带宽（字节）
   */
  public DataTransferThrottler(long bandwidthPerSec) {
    this(500, bandwidthPerSec);  // by default throttling period is 500ms 
  }

  /**
   * 全参数构造函数，自定义统计周期和带宽限制
   * @param period 统计周期（毫秒），带宽按此周期进行限制
   * @param bandwidthPerSec 允许的每秒带宽（字节）
   */
  public DataTransferThrottler(long period, long bandwidthPerSec) {
    this.curPeriodStart = monotonicNow();
    this.period = period;
    this.curReserve = this.bytesPerPeriod = bandwidthPerSec*period/1000;
    this.periodExtension = period*3;
  }

  /**
   * 获取当前设置的带宽限制
   * @return 当前限制带宽，单位：字节/秒
   */
  public synchronized long getBandwidth() {
    return bytesPerPeriod*1000/period;
  }
  
  /**
   * 更新带宽限制，修改将在当前周期结束后生效
   * @param bytesPerSecond 新的带宽限制，单位：字节/秒
   */
  public synchronized void setBandwidth(long bytesPerSecond) {
    if ( bytesPerSecond <= 0 ) {
      throw new IllegalArgumentException("" + bytesPerSecond);
    }
    bytesPerPeriod = bytesPerSecond*period/1000;
  }
  
  /**
   * 根据当前已传输字节数进行流量限制，如果超过带宽则阻塞当前线程
   * @param numOfBytes 自上次调用该方法以来，传输的字节数
   */
  public synchronized void throttle(long numOfBytes) {
    throttle(numOfBytes, null);
  }

  /**
   * 根据当前已传输字节数进行流量限制，支持外部取消中断
   * 如果当前传输速率超过设定带宽，会让当前线程阻塞等待，直到速率符合限制
   * @param numOfBytes 自上次调用该方法以来，传输的字节数
   * @param canceler 可选的取消器，用于提前终止限流等待
   */
  public synchronized void throttle(long numOfBytes, Canceler canceler) {
    if ( numOfBytes <= 0 ) {
      return;
    }

    // 扣减当前周期剩余可用字节数
    curReserve -= numOfBytes;
    bytesAlreadyUsed += numOfBytes;

    // 当剩余可用字节小于等于0时，需要限流等待
    while (curReserve <= 0) {
      // 检查是否已被外部取消，若取消则直接退出限流
      if (canceler != null && canceler.isCancelled()) {
        return;
      }
      long now = monotonicNow();
      long curPeriodEnd = curPeriodStart + period;

      if ( now < curPeriodEnd ) {
        // 当前周期未结束，等待到周期结束后再允许传输
        try {
          wait( curPeriodEnd - now );
        } catch (InterruptedException e) {
          // 中断后中止限流，恢复中断状态让上层调用栈处理中断
          Thread.currentThread().interrupt();
          break;
        }
      } else if ( now <  (curPeriodStart + periodExtension)) {
        // 进入下一个周期，累加新周期的可用字节数
        curPeriodStart = curPeriodEnd;
        curReserve += bytesPerPeriod;
      } else {
        // 长时间未使用限流，丢弃之前周期，重置统计信息
        curPeriodStart = now;
        curReserve = bytesPerPeriod - bytesAlreadyUsed;
      }
    }

    // 扣除本次统计的字节数，为下一次限流计算做准备
    bytesAlreadyUsed -= numOfBytes;
  }
}