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
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.speculate.forecast.SimpleExponentialSmoothing;

/**
 * 文件说明：基于指数平滑算法的任务运行时间预估器，为MapReduce推测执行功能提供任务运行时间预测能力
 * 
 * 基于指数平滑算法实现的任务运行时间预估器，通过追踪任务进度变化来预测任务剩余运行时间，
 * 支持识别进度停滞任务，为推测启动冗余任务提供决策依据。
 */
public class SimpleExponentialTaskRuntimeEstimator extends StartEndTimesBase {

  /**
   * 无历史记录时默认返回的预估运行时间值，表示预估失败
   */
  private static final long DEFAULT_ESTIMATE_RUNTIME = -1L;

  /**
   * 当预测进度速率为0时使用的默认值，避免除零异常
   */
  private static final double DEFAULT_PROGRESS_VALUE = 1E-10;

  /**
   * 计算置信区间时使用的系数，用于新任务预估时增加安全裕度
   */
  private static final double CONFIDENCE_INTERVAL_FACTOR = 0.25;

  /**
   * 计算指数平滑系数时使用的常量时间参数，控制平滑敏感度
   */
  private long constTime;

  /**
   * 预估结果稳定前需要跳过的初始读数次数，避免初始值导致预估偏差
   */
  private int skipCount;

  /**
   * 判断任务进度停滞的时间窗口，若超过该窗口无进度更新则判定为停滞
   */
  private long stagnatedWindow;

  /**
   * 任务尝试ID对应指数平滑统计模型的缓存，线程安全存储各任务的预测模型
   */
  private final ConcurrentMap<TaskAttemptId,
      AtomicReference<SimpleExponentialSmoothing>>
      estimates = new ConcurrentHashMap<>();

  /**
   * 根据任务尝试ID获取对应的指数平滑预测模型实例
   * @param attemptID 任务尝试ID
   * @return 对应的预测模型实例，不存在则返回null
   */
  private SimpleExponentialSmoothing getForecastEntry(
      final TaskAttemptId attemptID) {
    AtomicReference<SimpleExponentialSmoothing> entryRef = estimates
        .get(attemptID);
    if (entryRef == null) {
      return null;
    }
    return entryRef.get();
  }

  /**
   * 整合新的进度读数到指数平滑模型中，更新预测结果
   * @param attemptID 目标任务尝试ID
   * @param newRawData 新的进度值（0~1）
   * @param newTimeStamp 新读数对应的时间戳
   */
  private void incorporateReading(final TaskAttemptId attemptID,
      final float newRawData, final long newTimeStamp) {
    SimpleExponentialSmoothing foreCastEntry = getForecastEntry(attemptID);
    if (foreCastEntry == null) {
      Long tStartTime = startTimes.get(attemptID);
      // 任务尚未开始计时，跳过本次更新
      if (tStartTime == null) {
        return;
      }
      // 模型不存在则创建新模型后重新更新
      estimates.putIfAbsent(attemptID,
          new AtomicReference<>(SimpleExponentialSmoothing.createForecast(
              constTime, skipCount, stagnatedWindow,
              tStartTime)));
      incorporateReading(attemptID, newRawData, newTimeStamp);
      return;
    }
    foreCastEntry.incorporateReading(newTimeStamp, newRawData);
  }

  /**
   * 初始化预估器，从配置中加载参数
   * @param conf 作业配置对象
   * @param context MR应用上下文
   */
  @Override
  public void contextualize(final Configuration conf,
      final AppContext context) {
    super.contextualize(conf, context);

    // 加载指数平滑常量时间参数
    constTime
        = conf.getLong(MRJobConfig.MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_LAMBDA_MS,
        MRJobConfig.DEFAULT_MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_LAMBDA_MS);

    // 加载进度停滞判断窗口，不小于两倍常量时间
    stagnatedWindow = Math.max(2 * constTime, conf.getLong(
        MRJobConfig.MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_STAGNATED_MS,
        MRJobConfig.DEFAULT_MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_STAGNATED_MS));

    // 加载初始跳过读数次数配置
    skipCount = conf
        .getInt(MRJobConfig.MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_SKIP_INITIALS,
            MRJobConfig.DEFAULT_MR_AM_TASK_ESTIMATOR_SIMPLE_SMOOTH_INITIALS);
  }

  /**
   * 估算指定任务尝试的总运行时间
   * @param id 目标任务尝试ID
   * @return 预估总运行时间（毫秒），无模型则返回默认-1
   */
  @Override
  public long estimatedRuntime(final TaskAttemptId id) {
    SimpleExponentialSmoothing foreCastEntry = getForecastEntry(id);
    if (foreCastEntry == null) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }
    // 计算剩余工作量，限制在0~1范围内
    double remainingWork = Math
        .max(0.0, Math.min(1.0, 1.0 - foreCastEntry.getRawData()));
    // 获取进度速率，避免为0导致除零
    double forecast = Math
        .max(DEFAULT_PROGRESS_VALUE, foreCastEntry.getForecast());
    // 计算剩余时间
    long remainingTime = (long) (remainingWork / forecast);
    // 总预估时间 = 已运行时间 + 剩余时间
    long estimatedRuntime = remainingTime
        + foreCastEntry.getTimeStamp()
        - foreCastEntry.getStartTime();
    return estimatedRuntime;
  }

  /**
   * 预估同任务新启动尝试的运行时间，基于同任务已完成尝试的统计数据
   * @param id 目标任务ID
   * @return 新尝试预估运行时间（毫秒），无统计数据则返回默认-1
   */
  @Override
  public long estimatedNewAttemptRuntime(final TaskId id) {
    DataStatistics statistics = dataStatisticsForTask(id);

    if (statistics == null) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }

    // 在均值基础上增加置信裕度，避免低估运行时间
    double statsMeanCI = statistics.meanCI();
    double expectedVal =
        statsMeanCI + Math.min(statsMeanCI * CONFIDENCE_INTERVAL_FACTOR,
            statistics.std() / 2);
    return (long) (expectedVal);
  }

  /**
   * 判断指定任务尝试是否进度停滞
   * @param id 目标任务尝试ID
   * @param timeStamp 当前时间戳
   * @return true表示进度停滞，false表示正常推进
   */
  @Override
  public boolean hasStagnatedProgress(final TaskAttemptId id,
      final long timeStamp) {
    SimpleExponentialSmoothing foreCastEntry = getForecastEntry(id);
    if (foreCastEntry == null) {
      return false;
    }
    return foreCastEntry.isDataStagnated(timeStamp);
  }

  /**
   * 获取任务运行时间预估的方差，当前版本未实现该功能
   * @param id 目标任务尝试ID
   * @return 固定返回0，无模型时返回-1
   */
  @Override
  public long runtimeEstimateVariance(final TaskAttemptId id) {
    SimpleExponentialSmoothing forecastEntry = getForecastEntry(id);
    if (forecastEntry == null) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }
    double forecast = forecastEntry.getForecast();
    if (forecastEntry.isDefaultForecast(forecast)) {
      return DEFAULT_ESTIMATE_RUNTIME;
    }
    //TODO What is the best way to measure variance in runtime
    return 0L;
  }

  /**
   * 更新任务尝试的进度状态，整合最新进度数据到预测模型
   * @param status 任务尝试最新状态
   * @param timestamp 更新时间戳
   */
  @Override
  public void updateAttempt(final TaskAttemptStatus status,
      final long timestamp) {
    super.updateAttempt(status, timestamp);
    TaskAttemptId attemptID = status.id;

    float progress = status.progress;

    incorporateReading(attemptID, progress, timestamp);
  }
}