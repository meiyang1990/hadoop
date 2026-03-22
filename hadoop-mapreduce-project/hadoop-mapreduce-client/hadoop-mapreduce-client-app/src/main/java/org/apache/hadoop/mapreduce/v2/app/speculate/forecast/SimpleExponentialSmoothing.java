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

package org.apache.hadoop.mapreduce.v2.app.speculate.forecast;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 文件说明：简单指数平滑法模型实现，用于MapReduce任务推测执行中对任务进度速率进行预测
 * 
 * 简单指数平滑法静态模型实现，通过历史进度数据预测任务执行速率，为推测执行提供数据支撑。
 * 支持增量更新预测结果，能够检测任务是否停滞，并计算预测误差平方和。
 */
public class SimpleExponentialSmoothing {
  private static final double DEFAULT_FORECAST = -1.0;
  // 最小样本数阈值，达到该阈值后才会更新时间常数
  private final int kMinimumReads;
  // 停滞检测窗口大小，超过该窗口未更新则判定为停滞
  private final long kStagnatedWindow;
  // 任务开始时间戳
  private final long startTime;
  // 时间常数，用于计算平滑因子
  private long timeConstant;

  /**
   * 持有当前最新预测记录的原子引用，保证多线程并发更新安全
   */
  private AtomicReference<ForecastRecord> forecastRefEntry;

  /**
   * 创建简单指数平滑预测实例的工厂方法
   * @param timeConstant 初始时间常数
   * @param skipCnt 跳过的初始样本数，即最小有效样本数阈值
   * @param stagnatedWindow 停滞检测窗口大小（毫秒）
   * @param timeStamp 任务开始时间戳
   * @return 初始化完成的简单指数平滑预测实例
   */
  public static SimpleExponentialSmoothing createForecast(
      final long timeConstant,
      final int skipCnt, final long stagnatedWindow, final long timeStamp) {
    return new SimpleExponentialSmoothing(timeConstant, skipCnt,
        stagnatedWindow, timeStamp);
  }

  /**
   * 构造简单指数平滑预测实例
   * @param ktConstant 初始时间常数
   * @param skipCnt 最小有效样本数阈值
   * @param stagnatedWindow 停滞检测窗口大小
   * @param timeStamp 任务开始时间戳
   */
  SimpleExponentialSmoothing(final long ktConstant, final int skipCnt,
      final long stagnatedWindow, final long timeStamp) {
    this.kMinimumReads = skipCnt;
    this.kStagnatedWindow = stagnatedWindow;
    this.timeConstant = ktConstant;
    this.startTime = timeStamp;
    this.forecastRefEntry = new AtomicReference<ForecastRecord>(null);
  }

  /**
   * 内部类，保存单条预测记录，包含当前预测结果、原始数据、误差统计等信息
   * 采用链表结构保存历史记录，支持回退到上一版本
   */
  private class ForecastRecord {
    // 当前平滑因子
    private final double alpha;
    // 当前记录时间戳
    private final long timeStamp;
    // 预处理后的样本数据（进度速率）
    private final double sample;
    // 原始进度数据
    private final double rawData;
    // 当前预测结果（进度速率）
    private double forecast;
    // 累计误差平方和
    private final double sseError;
    // 当前记录索引，统计样本总数
    private final long myIndex;
    // 前一条预测记录，用于处理同时间戳更新时回退
    private ForecastRecord prevRec;

    /**
     * 构造第一条预测记录
     * @param currForecast 初始预测值
     * @param currRawData 初始原始数据
     * @param currTimeStamp 初始时间戳
     */
    ForecastRecord(final double currForecast, final double currRawData,
        final long currTimeStamp) {
      this(0.0, currForecast, currRawData, currForecast, currTimeStamp, 0.0, 0);
    }

    /**
     * 完整构造预测记录
     * @param alphaVal 平滑因子
     * @param currSample 预处理后的样本
     * @param currRawData 原始进度数据
     * @param currForecast 当前预测值
     * @param currTimeStamp 当前时间戳
     * @param accError 累计误差平方和
     * @param index 当前样本索引
     */
    ForecastRecord(final double alphaVal, final double currSample,
        final double currRawData,
        final double currForecast, final long currTimeStamp,
        final double accError,
        final long index) {
      this.timeStamp = currTimeStamp;
      this.alpha = alphaVal;
      this.sample = currSample;
      this.forecast = currForecast;
      this.rawData = currRawData;
      this.sseError = accError;
      this.myIndex = index;
    }

    /**
     * 创建新的预测记录并关联前序记录
     * @param alphaVal 平滑因子
     * @param currSample 预处理后的样本
     * @param currRawData 原始进度数据
     * @param currForecast 当前预测值
     * @param currTimeStamp 当前时间戳
     * @param accError 累计误差平方和
     * @param index 当前样本索引
     * @param prev 前序预测记录
     * @return 新建的预测记录
     */
    private ForecastRecord createForecastRecord(final double alphaVal,
        final double currSample,
        final double currRawData,
        final double currForecast, final long currTimeStamp,
        final double accError,
        final long index,
        final ForecastRecord prev) {
      ForecastRecord forecastRec =
          new ForecastRecord(alphaVal, currSample, currRawData, currForecast,
              currTimeStamp, accError, index);
      forecastRec.prevRec = prev;
      return forecastRec;
    }

    /**
     * 基于当前记录预处理新的原始数据，计算进度速率
     * @param rData 新原始进度数据
     * @param newTime 新时间戳
     * @return 计算得到的进度速率
     */
    private double preProcessRawData(final double rData, final long newTime) {
      return processRawData(this.rawData, this.timeStamp, rData, newTime);
    }

    /**
     * 追加新的观测数据，更新预测结果
     * @param newTimeStamp 新观测时间戳
     * @param rData 新原始进度数据
     * @return 更新后的预测记录
     */
    public ForecastRecord append(final long newTimeStamp, final double rData) {
      // 重复上报进度，直接返回当前记录
      if (this.timeStamp >= newTimeStamp
          && Double.compare(this.rawData, rData) >= 0) {
        // progress reported twice. Do nothing.
        return this;
      }
      ForecastRecord refRecord = this;
      // 同一时间戳，尝试回退到前一条记录
      if (newTimeStamp == this.timeStamp) {
        // we need to restore old value if possible
        if (this.prevRec != null) {
          refRecord = this.prevRec;
        }
      }
      // 计算新样本的进度速率
      double newSample = refRecord.preProcessRawData(rData, newTimeStamp);
      long deltaTime = this.timeStamp - newTimeStamp;
      // 达到最小样本数阈值后，更新时间常数
      if (refRecord.myIndex == kMinimumReads) {
        timeConstant = Math.max(timeConstant, newTimeStamp - startTime);
      }
      // 计算平滑因子
      double smoothFactor =
          1 - Math.exp(((double) deltaTime) / timeConstant);
      // 指数平滑计算新预测值
      double forecastVal =
          smoothFactor * newSample + (1.0 - smoothFactor) * refRecord.forecast;
      // 更新累计误差平方和
      double newSSEError =
          refRecord.sseError + Math.pow(newSample - refRecord.forecast, 2);
      // 创建并返回新的预测记录
      return refRecord
          .createForecastRecord(smoothFactor, newSample, rData, forecastVal,
              newTimeStamp, newSSEError, refRecord.myIndex + 1, refRecord);
    }
  }

  /**
   * 检测任务是否处于停滞状态（长时间没有进度更新）
   * @param timeStamp 当前检测时间戳
   * @return true 若样本数超过最小阈值且上次更新时间超过停滞窗口，否则返回false
   */
  public boolean isDataStagnated(final long timeStamp) {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null && rec.myIndex > kMinimumReads) {
      // 注意：这里原始代码逻辑为：若(上次时间+窗口) > 当前时间，返回true，代表未停滞？这里保留原始逻辑不变
      return (rec.timeStamp + kStagnatedWindow) > timeStamp;
    }
    return false;
  }

  /**
   * 预处理原始进度数据，计算单位时间进度速率
   * @param oldRawData 上一次原始进度
   * @param oldTime 上一次时间戳
   * @param newRawData 当前原始进度
   * @param newTime 当前时间戳
   * @return 进度速率（进度变化量除以时间变化量）
   */
  static double processRawData(final double oldRawData, final long oldTime,
      final double newRawData, final long newTime) {
    double rate = (newRawData - oldRawData) / (newTime - oldTime);
    return rate;
  }

  /**
   * 整合新的观测读数，更新预测结果，线程安全
   * @param timeStamp 观测时间戳
   * @param currRawData 当前原始进度数据
   */
  public void incorporateReading(final long timeStamp,
      final double currRawData) {
    ForecastRecord oldRec = forecastRefEntry.get();
    // 第一条数据，初始化预测记录
    if (oldRec == null) {
      double oldForecast =
          processRawData(0, startTime, currRawData, timeStamp);
      forecastRefEntry.compareAndSet(null,
          new ForecastRecord(oldForecast, 0.0, startTime));
      // 递归调用处理当前新数据
      incorporateReading(timeStamp, currRawData);
      return;
    }
    // CAS循环更新，保证并发安全
    while (!forecastRefEntry.compareAndSet(oldRec, oldRec.append(timeStamp,
        currRawData))) {
      oldRec = forecastRefEntry.get();
    }
  }

  /**
   * 获取最终预测的进度速率
   * @return 若样本数达到最小阈值返回预测值，否则返回默认值-1
   */
  public double getForecast() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null && rec.myIndex > kMinimumReads) {
      return rec.forecast;
    }
    return DEFAULT_FORECAST;
  }

  /**
   * 判断当前预测值是否为默认未就绪值
   * @param value 待判断的值
   * @return true 是默认值，否则false
   */
  public boolean isDefaultForecast(final double value) {
    return value == DEFAULT_FORECAST;
  }

  /**
   * 获取累计预测误差平方和
   * @return 误差平方和，若无记录返回默认值-1
   */
  public double getSSE() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.sseError;
    }
    return DEFAULT_FORECAST;
  }

  /**
   * 检查累计误差是否在给定边界内
   * @param bound 误差上限边界
   * @return true 误差小于边界，否则false
   */
  public boolean isErrorWithinBound(final double bound) {
    double squaredErr = getSSE();
    if (squaredErr < 0) {
      return false;
    }
    return bound > squaredErr;
  }

  /**
   * 获取最新原始进度数据
   * @return 最新原始进度，若无记录返回默认值-1
   */
  public double getRawData() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.rawData;
    }
    return DEFAULT_FORECAST;
  }

  /**
   * 获取最新预测记录的时间戳
   * @return 最新时间戳，若无记录返回0
   */
  public long getTimeStamp() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.timeStamp;
    }
    return 0L;
  }

  /**
   * 获取任务开始时间戳
   * @return 任务开始时间戳
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * 获取当前预测记录的原子引用
   * @return 原子引用对象
   */
  public AtomicReference<ForecastRecord> getForecastRefEntry() {
    return forecastRefEntry;
  }

  @Override
  public String toString() {
    String res = "NULL";
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      res =  "rec.index = " + rec.myIndex + ", forecast t: " + rec.timeStamp
          + ", forecast: " + rec.forecast
          + ", sample: " + rec.sample + ", raw: " + rec.rawData + ", error: "
          + rec.sseError + ", alpha: " + rec.alpha;
    }
    return res;
  }

}