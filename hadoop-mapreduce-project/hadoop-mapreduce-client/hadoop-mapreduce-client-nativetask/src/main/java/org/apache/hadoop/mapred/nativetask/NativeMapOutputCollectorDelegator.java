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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.handlers.NativeCollectorOnlyHandler;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.QuickSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件: NativeMapOutputCollectorDelegator.java
 * 所属模块: Hadoop MapReduce 本地任务模块
 * 核心职责: 封装C++实现的Native Map输出收集器，向上提供标准Java MapOutputCollector接口，
 * 实现Map阶段输出数据的Native化处理，提升排序和收集性能
 */
@InterfaceAudience.Private
public class NativeMapOutputCollectorDelegator<K, V> implements MapOutputCollector<K, V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(NativeMapOutputCollectorDelegator.class);
  private JobConf job;
  private NativeCollectorOnlyHandler<K, V> handler;

  private Context context;
  private StatusReportChecker updater;

  /**
   * 收集Map任务输出的键值对，委托给Native处理器处理
   * @param key 输出键
   * @param value 输出值
   * @param partition 分区编号
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void collect(K key, V value, int partition) throws IOException, InterruptedException {
    handler.collect(key, value, partition);
  }

  /**
   * 关闭Native输出收集器，停止状态更新线程，上报最终状态
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void close() throws IOException, InterruptedException {
    handler.close();
    if (null != updater) {
      // 停止状态更新线程
      updater.stop();
      // 向Hadoop框架上报最终任务状态
      NativeRuntime.reportStatus(context.getReporter());
    }
  }

  /**
   * 刷新所有输出数据到磁盘，委托给Native处理器处理
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   * @throws ClassNotFoundException 类找不到异常
   */
  @Override
  public void flush() throws IOException, InterruptedException, ClassNotFoundException {
    handler.flush();
  }

  /**
   * 初始化Native Map输出收集器，完成环境检查和Native资源初始化
   * @param context Map任务上下文
   * @throws IOException IO异常
   * @throws ClassNotFoundException 类找不到异常
   */
  @SuppressWarnings("unchecked")
  @Override
  public void init(Context context) throws IOException, ClassNotFoundException {
    this.context = context;
    this.job = context.getJobConf();

    // 初始化Native平台环境
    Platforms.init(job);

    // 无Reduce阶段不需要使用Native输出收集器，直接报错
    if (job.getNumReduceTasks() == 0) {
      String message = "There is no reducer, no need to use native output collector";
      LOG.error(message);
      throw new InvalidJobConfException(message);
    }

    // 获取作业配置的Key比较器类
    Class<?> comparatorClass = job.getClass(MRJobConfig.KEY_COMPARATOR, null,
        RawComparator.class);
    // 不支持自定义Java比较器，必须使用原生可比较类型，报错退出
    if (comparatorClass != null && !Platforms.define(comparatorClass)) {
      String message = "Native output collector doesn't support customized java comparator "
        + job.get(MRJobConfig.KEY_COMPARATOR);
      LOG.error(message);
      throw new InvalidJobConfException(message);
    }


    // 只支持Hadoop原生QuickSort，不支持自定义排序类，报错退出
    if (!QuickSort.class.getName().equals(job.get(Constants.MAP_SORT_CLASS))) {
      String message = "Native-Task doesn't support sort class " +
        job.get(Constants.MAP_SORT_CLASS);
      LOG.error(message);
      throw new InvalidJobConfException(message);
    }

    // 不支持SSL加密Shuffle，报错退出
    if (job.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY, false) == true) {
      String message = "Native-Task doesn't support secure shuffle";
      LOG.error(message);
      throw new InvalidJobConfException(message);
    }

    // 获取Map输出Key类型
    final Class<?> keyCls = job.getMapOutputKeyClass();
    try {
      // 获取对应Key类型的Native序列化器
      @SuppressWarnings("rawtypes")
      final INativeSerializer serializer = NativeSerialization.getInstance().getSerializer(keyCls);
      // 找不到对应序列化器，报错退出
      if (null == serializer) {
        String message = "Key type not supported. Cannot find serializer for " + keyCls.getName();
        LOG.error(message);
        throw new InvalidJobConfException(message);
      } else if (!Platforms.support(keyCls.getName(), serializer, job)) {
        // Key类型不支持Native层比较，报错退出
        String message = "Native output collector doesn't support this key, " +
          "this key is not comparable in native: " + keyCls.getName();
        LOG.error(message);
        throw new InvalidJobConfException(message);
      }
    } catch (final IOException e) {
      // 获取序列化器失败，报错退出
      String message = "Cannot find serializer for " + keyCls.getName();
      LOG.error(message);
      throw new IOException(message);
    }

    // 检查Native库是否加载成功
    final boolean ret = NativeRuntime.isNativeLibraryLoaded();
    if (ret) {
      // 如果开启了Map输出压缩，检查压缩编解码器是否被Native支持
      if (job.getBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false)) {
        String codec = job.get(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC);
        if (!NativeRuntime.supportsCompressionCodec(codec.getBytes(StandardCharsets.UTF_8))) {
          String message = "Native output collector doesn't support compression codec " + codec;
          LOG.error(message);
          throw new InvalidJobConfException(message);
        }
      }
      // 将作业配置传递给Native层
      NativeRuntime.configure(job);

      // 初始化状态更新线程，定期向Hadoop框架上报任务状态
      final long updateInterval = job.getLong(Constants.NATIVE_STATUS_UPDATE_INTERVAL,
          Constants.NATIVE_STATUS_UPDATE_INTERVAL_DEFVAL);
      updater = new StatusReportChecker(context.getReporter(), updateInterval);
      updater.start();

    } else {
      // Native库加载失败，报错退出
      String message = "NativeRuntime cannot be loaded, please check that " +
        "libnativetask.so is in hadoop library dir";
      LOG.error(message);
      throw new InvalidJobConfException(message);
    }

    this.handler = null;
    try {
      // 获取输出键值对类型
      final Class<K> oKClass = (Class<K>) job.getMapOutputKeyClass();
      final Class<K> oVClass = (Class<K>) job.getMapOutputValueClass();
      // 获取任务尝试ID
      final TaskAttemptID id = context.getMapTask().getTaskID();
      // 构建Native任务上下文
      final TaskContext taskContext = new TaskContext(job, null, null, oKClass, oVClass,
          context.getReporter(), id);
      // 创建Native输出收集处理器
      handler = NativeCollectorOnlyHandler.create(taskContext);
    } catch (final IOException e) {
      // 创建Native处理器失败，报错退出
      String message = "Native output collector cannot be loaded;";
      LOG.error(message);
      throw new IOException(message, e);
    }

    LOG.info("Native output collector can be successfully enabled!");
  }

}