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
package org.apache.hadoop.mapreduce.util;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 文件级注释：MapReduce作业配置工具类，提供作业配置敏感信息脱敏、进度报告间隔计算、任务进度日志控
 * 制、以及单元测试环境配置等通用工具能力，供MapReduce客户端和核心模块使用。
 * 
 * A class that contains utility methods for MR Job configuration.
 */
public final class MRJobConfUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(MRJobConfUtil.class);
  public static final String REDACTION_REPLACEMENT_VAL = "*********(redacted)";

  /**
   * 对作业配置中的敏感属性进行脱敏处理，将配置项替换为固定脱敏值，避免敏感信息泄露。
   * Redact job configuration properties.
   * @param conf the job configuration to redact
   */
  public static void redact(final Configuration conf) {
    for (String prop : conf.getTrimmedStringCollection(
        MRJobConfig.MR_JOB_REDACTED_PROPERTIES)) {
      conf.set(prop, REDACTION_REPLACEMENT_VAL);
    }
  }

  /**
   * 工具类不允许实例化。
   * There is no reason to instantiate this utility class.
   */
  private MRJobConfUtil() {
  }

  /**
   * 计算MapReduce任务的进度心跳报告间隔，默认值为任务超时时间的固定比例。
   * Get the progress heartbeat interval configuration for mapreduce tasks.
   * By default, the value of progress heartbeat interval is a proportion of
   * that of task timeout.
   * @param conf  the job configuration to read from
   * @return the value of task progress report interval
   */
  public static long getTaskProgressReportInterval(final Configuration conf) {
    // 读取任务超时配置
    long taskHeartbeatTimeOut = conf.getLong(
        MRJobConfig.TASK_TIMEOUT, MRJobConfig.DEFAULT_TASK_TIMEOUT_MILLIS);
    // 优先使用用户配置的间隔，否则按比例计算默认值
    return conf.getLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL,
        (long) (TASK_REPORT_INTERVAL_TO_TIMEOUT_RATIO * taskHeartbeatTimeOut));
  }

  public static final float TASK_REPORT_INTERVAL_TO_TIMEOUT_RATIO = 0.01f;

  /**
   * 任务尝试进度日志的最小进度变化系数，用于控制日志输出频率。
   * Configurations to control the frequency of logging of task Attempt.
   */
  public static final double PROGRESS_MIN_DELTA_FACTOR = 100.0;
  private static volatile Double progressMinDeltaThreshold = null;
  private static volatile Long progressMaxWaitDeltaTimeThreshold = null;

  /**
   * 从配置文件加载任务进度日志的阈值配置，包括最小进度变化阈值和最大日志间隔时间，采用懒加载
   * 方式初始化阈值。
   * load the values defined from a configuration file including the delta
   * progress and the maximum time between each log message.
   * @param conf 作业配置对象
   */
  public static void setTaskLogProgressDeltaThresholds(
      final Configuration conf) {
    // 双重检查锁定实现懒加载线程安全初始化
    if (progressMinDeltaThreshold == null) {
      progressMinDeltaThreshold =
          new Double(PROGRESS_MIN_DELTA_FACTOR
              * conf.getDouble(MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD,
              MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD_DEFAULT));
    }

    if (progressMaxWaitDeltaTimeThreshold == null) {
      progressMaxWaitDeltaTimeThreshold =
          TimeUnit.SECONDS.toMillis(conf
              .getLong(
                  MRJobConfig.TASK_LOG_PROGRESS_WAIT_INTERVAL_SECONDS,
                  MRJobConfig.TASK_LOG_PROGRESS_WAIT_INTERVAL_SECONDS_DEFAULT));
    }
  }

  /**
   * 获取触发进度日志输出所需的最小进度变化阈值，只有进度变化超过该阈值才会输出日志。
   * Retrieves the min delta progress required to log the task attempt current
   * progress.
   * @return the defined threshold in the conf.
   *         returns the default value if
   *         {@link #setTaskLogProgressDeltaThresholds} has not been called.
   */
  public static double getTaskProgressMinDeltaThreshold() {
    if (progressMinDeltaThreshold == null) {
      return PROGRESS_MIN_DELTA_FACTOR
          * MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD_DEFAULT;
    }
    return progressMinDeltaThreshold.doubleValue();
  }

  /**
   * 获取两次进度日志输出之间必须间隔的最小时间阈值，避免日志输出过于频繁。
   * Retrieves the min time required to log the task attempt current
   * progress.
   * @return the defined threshold in the conf.
   *         returns the default value if
   *         {@link #setTaskLogProgressDeltaThresholds} has not been called.
   */
  public static long getTaskProgressWaitDeltaTimeThreshold() {
    if (progressMaxWaitDeltaTimeThreshold == null) {
      return TimeUnit.SECONDS.toMillis(
          MRJobConfig.TASK_LOG_PROGRESS_WAIT_INTERVAL_SECONDS_DEFAULT);
    }
    return progressMaxWaitDeltaTimeThreshold.longValue();
  }

  /**
   * 将0.0~1.0范围的任务进度转换为整数倍进度系数，用于进度日志比较。
   * Coverts a progress between 0.0 to 1.0 to double format used to log the
   * task attempt.
   * @param progress of the task which is a value between 0.0 and 1.0.
   * @return the double value that is less than or equal to the argument
   *          multiplied by {@link #PROGRESS_MIN_DELTA_FACTOR}.
   */
  public static double convertTaskProgressToFactor(final float progress) {
    return Math.floor(progress * MRJobConfUtil.PROGRESS_MIN_DELTA_FACTOR);
  }

  /**
   * 单元测试专用JVM安全参数，使用非阻塞熵源避免低熵系统上YarnChild进程挂起。
   * For unit tests, use urandom to avoid the YarnChild  process from hanging
   * on low entropy systems.
   */
  private static final String TEST_JVM_SECURITY_EGD_OPT =
      "-Djava.security.egd=file:/dev/./urandom";

  /**
   * 为单元测试初始化加密中间数据配置，添加测试专用JVM参数避免加密过程挂起。
   * @param conf 原始作业配置
   * @return 初始化后的测试配置
   */
  public static Configuration initEncryptedIntermediateConfigsForTesting(
      Configuration conf) {
    Configuration config =
        (conf == null) ? new Configuration(): conf;
    final String childJVMOpts =
        TEST_JVM_SECURITY_EGD_OPT.concat(" ")
            .concat(config.get("mapred.child.java.opts", " "));
    // 设置AM和子任务JVM参数
    config.set("yarn.app.mapreduce.am.admin-command-opts",
        TEST_JVM_SECURITY_EGD_OPT);
    config.set("mapred.child.java.opts", childJVMOpts);
    // 开启中间数据加密
    config.setBoolean("mapreduce.job.encrypted-intermediate-data", true);
    return config;
  }

  /**
   * 为单元测试配置本地目录，将所有临时目录设置为测试根目录的子目录，隔离测试数据。
   * Set local directories so that the generated folders is subdirectory of the
   * test directories.
   * @param conf 原始作业配置
   * @param testRootDir 单元测试根目录
   * @return 配置完成的测试配置
   */
  public static Configuration setLocalDirectoriesConfigForTesting(
      Configuration conf, File testRootDir) {
    Configuration config =
        (conf == null) ? new Configuration(): conf;
    final File hadoopLocalDir = new File(testRootDir, "hadoop-dir");
    // 创建本地目录，已存在则忽略
    if (!hadoopLocalDir.getAbsoluteFile().mkdirs()) {
      LOG.info("{} directory already exists", hadoopLocalDir.getPath());
    }
    // 构造各个本地目录路径
    Path mapredHadoopTempDir = new Path(hadoopLocalDir.getPath());
    Path mapredSystemDir = new Path(mapredHadoopTempDir, "system");
    Path stagingDir = new Path(mapredHadoopTempDir, "tmp/staging");
    // 将所有本地目录配置为测试目录的子目录
    config.set("mapreduce.jobtracker.staging.root.dir", stagingDir.toString());
    config.set("mapreduce.jobtracker.system.dir", mapredSystemDir.toString());
    config.set("mapreduce.cluster.temp.dir", mapredHadoopTempDir.toString());
    config.set("mapreduce.cluster.local.dir",
        new Path(mapredHadoopTempDir, "local").toString());
    return config;
  }
}