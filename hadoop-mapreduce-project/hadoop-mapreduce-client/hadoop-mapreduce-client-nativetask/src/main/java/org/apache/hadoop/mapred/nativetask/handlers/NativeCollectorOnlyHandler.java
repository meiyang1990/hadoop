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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.ICombineHandler;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.util.NativeTaskOutput;
import org.apache.hadoop.mapred.nativetask.util.OutputUtil;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 仅收集输出的Native任务处理器，用于Java层处理Mapper+Native层收集输出的混合执行场景
 * 对应执行流程：Java记录读取器 + Java Mapper + Native 收集器
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Private
public class NativeCollectorOnlyHandler<K, V> implements CommandDispatcher, Closeable {

  public static final String NAME = "NativeTask.MCollectorOutputHandler";
  private static final Logger LOG =
      LoggerFactory.getLogger(NativeCollectorOnlyHandler.class);
  // 获取输出文件路径命令
  public static final Command GET_OUTPUT_PATH =
      new Command(100, "GET_OUTPUT_PATH");
  // 获取输出索引文件路径命令
  public static final Command GET_OUTPUT_INDEX_PATH =
      new Command(101, "GET_OUTPUT_INDEX_PATH");
  // 获取溢写文件路径命令
  public static final Command GET_SPILL_PATH =
      new Command(102, "GET_SPILL_PATH");
  // 获取Combiner处理器命令
  public static final Command GET_COMBINE_HANDLER =
      new Command(103, "GET_COMBINE_HANDLER");
  
  private NativeTaskOutput output;
  private int spillNumber = 0;
  private ICombineHandler combinerHandler = null;
  private final BufferPusher<K, V> kvPusher;
  private final INativeHandler nativeHandler;
  private boolean closed = false;

  /**
   * 创建NativeCollectorOnlyHandler实例工厂方法
   * @param context 任务上下文
   * @return 创建完成的处理器实例
   * @throws IOException 创建过程异常
   */
  public static <K, V> NativeCollectorOnlyHandler<K, V> create(TaskContext context)
    throws IOException {

    
    ICombineHandler combinerHandler = null;
    try {
      // 复制任务上下文，修改输入类型为输出类型，适配Combiner输入需求
      final TaskContext combineContext = context.copyOf();
      combineContext.setInputKeyClass(context.getOutputKeyClass());
      combineContext.setInputValueClass(context.getOutputValueClass());

      // 创建Combiner处理器
      combinerHandler = CombinerHandler.create(combineContext);
    } catch (final ClassNotFoundException e) {
      throw new IOException(e);
    }
    
    if (null != combinerHandler) {
      LOG.info("[NativeCollectorOnlyHandler] combiner is not null");
    }

    // 创建Native处理器，用于输出方向数据处理
    final INativeHandler nativeHandler = NativeBatchProcessor.create(
      NAME, context.getConf(), DataChannel.OUT);
    // 创建KV数据推送器，将Java层产生的键值对推送给Native层
    final BufferPusher<K, V> kvPusher = new BufferPusher<K, V>(
        (Class<K>)context.getOutputKeyClass(),
        (Class<V>)context.getOutputValueClass(),
        nativeHandler);

    return new NativeCollectorOnlyHandler<K, V>(context, nativeHandler, kvPusher, combinerHandler);
  }

  /**
   * 构造函数，初始化Native收集器处理器
   * @param context 任务上下文
   * @param nativeHandler Native层处理器实例
   * @param kvPusher KV推送器实例
   * @param combiner Combiner处理器实例
   * @throws IOException 初始化异常
   */
  protected NativeCollectorOnlyHandler(TaskContext context, INativeHandler nativeHandler,
      BufferPusher<K, V> kvPusher, ICombineHandler combiner) throws IOException {
    Configuration conf = context.getConf();
    TaskAttemptID id = context.getTaskAttemptId();
    if (null == id) {
      // 空任务尝试ID，创建空输出对象
      this.output = OutputUtil.createNativeTaskOutput(conf, "");
    } else {
      // 根据任务尝试ID创建Native任务输出对象
      this.output = OutputUtil.createNativeTaskOutput(context.getConf(), context.getTaskAttemptId()
        .toString());
    }
    this.combinerHandler = combiner;
    this.kvPusher = kvPusher;
    this.nativeHandler = nativeHandler;
    // 设置当前实例为命令分发器，处理Native层的命令请求
    nativeHandler.setCommandDispatcher(this);
  }

  /**
   * 收集Java Mapper输出的键值对，推送给Native层处理
   * @param key 输出键
   * @param value 输出值
   * @param partition 分区编号
   * @throws IOException 收集过程IO异常
   */
  public void collect(K key, V value, int partition) throws IOException {
    kvPusher.collect(key, value, partition);
  };

  public void flush() throws IOException {
  }

  @Override
  /**
   * 关闭处理器，释放所有资源
   * @throws IOException 关闭过程IO异常
   */
  public void close() throws IOException {
    if (closed) {
      return;
    }

    if (null != kvPusher) {
      kvPusher.close();
    }

    if (null != combinerHandler) {
      combinerHandler.close();
    }

    if (null != nativeHandler) {
      nativeHandler.close();
    }
    closed = true;
  }

  @Override
  /**
   * 处理Native层发起的命令调用，返回对应结果
   * @param command 调用命令
   * @param parameter 调用参数
   * @return 命令处理结果缓冲区
   * @throws IOException 命令处理异常
   */
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
    Path p = null;
    if (null == command) {
      return null;
    }
        
    if (command.equals(GET_OUTPUT_PATH)) {
      // 获取最终输出文件路径
      p = output.getOutputFileForWrite(-1);
    } else if (command.equals(GET_OUTPUT_INDEX_PATH)) {
      // 获取最终输出索引文件路径
      p = output.getOutputIndexFileForWrite(-1);
    } else if (command.equals(GET_SPILL_PATH)) {
      // 获取溢写文件路径，溢写编号自增
      p = output.getSpillFileForWrite(spillNumber++, -1);
      
    } else if (command.equals(GET_COMBINE_HANDLER)) {
      // 返回Combiner处理器ID
      if (null == combinerHandler) {
        return null;
      }
      final ReadWriteBuffer result = new ReadWriteBuffer(8);
      
      result.writeLong(combinerHandler.getId());
      return result;
    } else {
      throw new IOException("Illegal command: " + command.toString());
    }
    if (p != null) {
      // 将路径写入缓冲区返回给Native层
      final ReadWriteBuffer result = new ReadWriteBuffer();
      result.writeString(p.toUri().getPath());
      return result;
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
    }
  }
}