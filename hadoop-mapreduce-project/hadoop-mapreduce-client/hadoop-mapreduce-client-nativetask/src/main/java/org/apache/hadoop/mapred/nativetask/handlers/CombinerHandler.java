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
package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.ICombineHandler;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.serde.SerializationFramework;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 原生任务的Combiner处理器，桥接Java层Combiner与原生层数据处理
 * 负责从原生层拉取Map输出数据，交给Java Combiner处理，再将结果推回原生层
 * 
 * @param <K> 键类型
 * @param <V> 值类型
 */
class CombinerHandler<K, V> implements ICombineHandler, CommandDispatcher {
  public static final String NAME = "NativeTask.CombineHandler";
  private static final Logger LOG =
      LoggerFactory.getLogger(NativeCollectorOnlyHandler.class);
  // 加载命令常量
  public static final Command LOAD = new Command(1, "Load");
  // 合并处理命令常量
  public static final Command COMBINE = new Command(4, "Combine");
  // Java层Combiner运行器实例
  public final CombinerRunner<K, V> combinerRunner;

  private final INativeHandler nativeHandler;
  private final BufferPuller puller;
  private final BufferPusher<K, V> kvPusher;
  private boolean closed = false;

  /**
   * 创建Combiner处理器实例，检查配置并初始化各组件
   * 
   * @param context 任务上下文，包含配置和任务信息
   * @return 初始化好的Combiner处理器实例，如果未配置Combiner则返回null
   * @throws IOException IO异常
   * @throws ClassNotFoundException 类加载异常
   */
  public static <K, V> ICombineHandler create(TaskContext context)
    throws IOException, ClassNotFoundException {
    final JobConf conf = new JobConf(context.getConf());
    // 设置序列化框架为Writable序列化
    conf.set(Constants.SERIALIZATION_FRAMEWORK,
        String.valueOf(SerializationFramework.WRITABLE_SERIALIZATION.getType()));
    // 从配置中获取Combiner类名
    String combinerClazz = conf.get(Constants.MAPRED_COMBINER_CLASS);
    if (null == combinerClazz) {
      combinerClazz = conf.get(MRJobConfig.COMBINE_CLASS_ATTR);
    }

    if (null == combinerClazz) {
      // 未配置Combiner，返回null
      return null;
    } else {
      LOG.info("NativeTask Combiner is enabled, class = " + combinerClazz);
    }

    // 获取Combiner输入记录计数器
    final Counter combineInputCounter = context.getTaskReporter().getCounter(
        TaskCounter.COMBINE_INPUT_RECORDS);

    // 创建Java层Combiner运行器
    final CombinerRunner<K, V> combinerRunner = CombinerRunner.create(
        conf, context.getTaskAttemptId(),
        combineInputCounter, context.getTaskReporter(), null);

    // 创建原生处理器实例，绑定INOUT数据通道
    final INativeHandler nativeHandler = NativeBatchProcessor.create(
      NAME, conf, DataChannel.INOUT);
    @SuppressWarnings("unchecked")
    // 创建Combiner结果推送到原生层的推送器
    final BufferPusher<K, V> pusher = new BufferPusher<K, V>((Class<K>)context.getInputKeyClass(),
        (Class<V>)context.getInputValueClass(),
        nativeHandler);
    // 创建从原生拉取数据的拉取器
    final BufferPuller puller = new BufferPuller(nativeHandler);
    return new CombinerHandler<K, V>(nativeHandler, combinerRunner, puller, pusher);
  }

  /**
   * 构造Combiner处理器，绑定各组件并注册命令分发器和数据接收者
   * 
   * @param nativeHandler 原生处理器实例
   * @param combiner Java层Combiner运行器
   * @param puller 原生数据拉取器
   * @param kvPusher 结果推送到原生的推送器
   * @throws IOException IO异常
   */
  public CombinerHandler(INativeHandler nativeHandler, CombinerRunner<K, V> combiner,
                         BufferPuller puller, BufferPusher<K, V> kvPusher)
    throws IOException {
    this.nativeHandler = nativeHandler;
    this.combinerRunner = combiner;
    this.puller = puller;
    this.kvPusher = kvPusher;
    nativeHandler.setCommandDispatcher(this);
    nativeHandler.setDataReceiver(puller);
  }

  /**
   * 处理原生层发来的命令，分发到对应处理逻辑
   * 
   * @param command 收到的命令
   * @param parameter 命令参数
   * @return 命令处理结果缓冲区
   * @throws IOException IO异常
   */
  @Override
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
 if (null == command) {
      return null;
    }
    // 如果是COMBINE命令，触发合并处理
    if (command.equals(COMBINE)) {
      combine();
    }
    return null;

  }

  /**
   * 执行Combiner合并处理流程：从原生拉取数据 -> Java Combiner合并 -> 将结果推回原生
   * 
   * @throws IOException IO异常
   */
  @Override
  public void combine() throws IOException{
    try {
      // 重置拉取器，准备读取新批次数据
      puller.reset();
      // 执行Combiner合并，拉取器作为输入，推送器作为输出
      combinerRunner.combine(puller, kvPusher);
      // 推送所有合并结果到原生层
      kvPusher.flush();
      return;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * 获取原生层处理器的地址ID
   * 
   * @return 原生处理器地址ID
   */
  @Override
  public long getId() {
    return nativeHandler.getNativeHandler();
  }

  /**
   * 关闭处理器，释放所有资源
   * 
   * @throws IOException IO异常
   */
  @Override
  public void close() throws IOException {

    if (closed) {
      return;
    }

    if (null != puller) {
      puller.close();
    }

    if (null != kvPusher) {
      kvPusher.close();
    }

    if (null != nativeHandler) {
      nativeHandler.close();
    }
    closed = true;
  }
}