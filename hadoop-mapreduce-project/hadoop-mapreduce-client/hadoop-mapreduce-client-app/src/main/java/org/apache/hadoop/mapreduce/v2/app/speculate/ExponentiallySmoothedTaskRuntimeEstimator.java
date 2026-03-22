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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;

/**
 * 文件说明：基于指数平滑算法的任务运行时间估算器，用于MapReduce推测执行功能
 * 
 * 本类实现了基于指数平滑的任务进度速率统计，通过对任务进度随时间变化的历史数据做平滑处理，
 * 估算任务完成所需的总运行时间，为推测执行判断慢任务提供数据支持。
 * 支持两种平滑模式：对进度速率做平滑，或对单位进度耗时做平滑。
 */
public class ExponentiallySmoothedTaskRuntimeEstimator extends StartEndTimesBase {

  // 存储每个任务尝试的估算向量，线程安全
  private final ConcurrentMap<TaskAttemptId, AtomicReference<EstimateVector>> estimates
      = new ConcurrentHashMap<TaskAttemptId, AtomicReference<EstimateVector>>();

  // 当前选择的平滑模式
  private SmoothedValue smoothedValue;

  // 指数平滑衰减时间窗口，单位毫秒
  private long lambda;

  /**
   * 平滑计算模式枚举
   */
  public enum SmoothedValue {
    /** 对进度速率（进度/时间）做指数平滑 */
    RATE, 
    /** 对单位进度耗时（时间/进度）做指数平滑 */
    TIME_PER_UNIT_PROGRESS
  }

  /**
   * 带参数构造函数，指定平滑衰减窗口和平滑模式
   * @param lambda 指数平滑衰减时间窗口，单位毫秒
   * @param smoothedValue 选择的平滑计算模式
   */
  ExponentiallySmoothedTaskRuntimeEstimator
      (long lambda, SmoothedValue smoothedValue) {
    super();
    this.smoothedValue = smoothedValue;
    this.lambda = lambda;
  }

  /**
   * 默认无参构造函数，后续通过contextualize方法从配置加载参数
   */
  public ExponentiallySmoothedTaskRuntimeEstimator() {
    super();
  }

  /**
   * 估算向量类，存储单个任务尝试的平滑估算结果，不可变
   * 保存当前平滑值、对应进度点和更新时间戳
   */
  // immutable
  private class EstimateVector {
    // 平滑计算后的目标值（速率或单位进度耗时）
    final double value;
    // 计算时基于的进度值
    final float basedOnProgress;
    // 本次更新的时间戳
    final long atTime;

    /**
     * 构造估算向量
     * @param value 平滑计算结果值
     * @param basedOnProgress 对应进度点
     * @param atTime 更新时间戳
     */
    EstimateVector(double value, float basedOnProgress, long atTime) {
      this.value = value;
      this.basedOnProgress = basedOnProgress;
      this.atTime = atTime;
    }

    /**
     * 纳入新的进度观测数据，更新指数平滑估算结果
     * @param newProgress 新的进度值
     * @param newAtTime 新进度对应的时间戳
     * @return 更新后的估算向量
     */
    EstimateVector incorporate(float newProgress, long newAtTime) {
      // 数据不合法（时间未前进或进度后退），返回原有估算
      if (newAtTime <= atTime || newProgress < basedOnProgress) {
        return this;
      }

      // 计算历史数据权重：越久的数据权重指数衰减
      double oldWeighting
          = value < 0.0
              ? 0.0 : Math.exp(((double) (newAtTime - atTime)) / lambda);

      // 计算本次观测到的速率：进度增量 / 时间增量
      double newRead = (newProgress - basedOnProgress) / (newAtTime - atTime);

      // 如果是单位进度耗时模式，转换为时间/进度
      if (smoothedValue == SmoothedValue.TIME_PER_UNIT_PROGRESS) {
        newRead = 1.0 / newRead;
      }

      // 指数平滑计算新值，返回新的不可变估算向量
      return new EstimateVector
          (value * oldWeighting + newRead * (1.0 - oldWeighting),
           newProgress, newAtTime);
    }
  }

  /**
   * 将新的进度观测数据纳入平滑计算，更新对应任务尝试的估算结果
   * @param attemptID 任务尝试ID
   * @param newProgress 新进度值
   * @param newTime 新进度对应时间戳
   */
  private void incorporateReading
      (TaskAttemptId attemptID, float newProgress, long newTime) {
    //TODO: Refactor this method, it seems more complicated than necessary.
    AtomicReference<EstimateVector> vectorRef = estimates.get(attemptID);

    // 首次遇到该任务尝试，初始化容器并递归处理
    if (vectorRef == null) {
      estimates.putIfAbsent(attemptID, new AtomicReference<EstimateVector>(null));
      incorporateReading(attemptID, newProgress, newTime);
      return;
    }

    EstimateVector oldVector = vectorRef.get();

    // 容器已创建但还未初始化，尝试初始化初始向量
    if (oldVector == null) {
      if (vectorRef.compareAndSet(null,
             new EstimateVector(-1.0, 0.0F, Long.MIN_VALUE))) {
        return;
      }
      // 初始化竞争失败，重试
      incorporateReading(attemptID, newProgress, newTime);
      return;
    }

    // CAS自旋更新，保证线程安全
    while (!vectorRef.compareAndSet
            (oldVector, oldVector.incorporate(newProgress, newTime))) {
      oldVector = vectorRef.get();
    }
  }

  /**
   * 获取指定任务尝试的估算向量
   * @param attemptID 任务尝试ID
   * @return 对应估算向量，不存在则返回null
   */
  private EstimateVector getEstimateVector(TaskAttemptId attemptID) {
    AtomicReference<EstimateVector> vectorRef = estimates.get(attemptID);

    if (vectorRef == null) {
      return null;
    }

    return vectorRef.get();
  }

  /**
   * 从配置中加载估算器参数，完成初始化
   * @param conf 作业配置对象
   * @param context 应用上下文对象
   */
  @Override
  public void contextualize(Configuration conf, AppContext context) {
    super.contextualize(conf, context);

    // 加载指数平滑衰减窗口配置
    lambda
        = conf.getLong(MRJobConfig.MR_AM_TASK_ESTIMATOR_SMOOTH_LAMBDA_MS,
            MRJobConfig.DEFAULT_MR_AM_TASK_ESTIMATOR_SMOOTH_LAMBDA_MS);
    // 加载平滑模式配置：默认使用速率平滑
    smoothedValue
        = conf.getBoolean(MRJobConfig.MR_AM_TASK_ESTIMATOR_EXPONENTIAL_RATE_ENABLE, true)
            ? SmoothedValue.RATE : SmoothedValue.TIME_PER_UNIT_PROGRESS;
  }

  /**
   * 估算指定任务尝试完成所需的总运行时间
   * @param id 任务尝试ID
   * @return 估算的总运行时间（毫秒），无法估算返回-1
   */
  @Override
  public long estimatedRuntime(TaskAttemptId id) {
    Long startTime = startTimes.get(id);

    if (startTime == null) {
      return -1L;
    }

    EstimateVector vector = getEstimateVector(id);

    if (vector == null) {
      return -1L;
    }

    // 已流逝时间 = 上次更新时间 - 任务启动时间
    long sunkTime = vector.atTime - startTime;

    double value = vector.value;
    float progress = vector.basedOnProgress;

    if (value == 0) {
      return -1L;
    }

    // 转换得到当前进度速率
    double rate = smoothedValue == SmoothedValue.RATE ? value : 1.0 / value;

    if (rate == 0.0) {
      return -1L;
    }

    // 剩余时间 = 剩余进度 / 进度速率
    double remainingTime = (1.0 - progress) / rate;

    // 总运行时间 = 已流逝时间 + 剩余时间
    return sunkTime + (long)remainingTime;
  }

  /**
   * 获取估算结果的方差，本实现不支持方差计算
   * @param id 任务尝试ID
   * @return 固定返回-1表示不提供方差数据
   */
  @Override
  public long runtimeEstimateVariance(TaskAttemptId id) {
    return -1L;
  }

  /**
   * 处理任务尝试状态更新，将新进度纳入估算更新
   * @param status 任务尝试最新状态
   * @param timestamp 更新时间戳
   */
  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {
    super.updateAttempt(status, timestamp);
    TaskAttemptId attemptID = status.id;

    float progress = status.progress;

    incorporateReading(attemptID, progress, timestamp);
  }
}