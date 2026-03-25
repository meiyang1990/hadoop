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
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.buffer.OutputBuffer;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件: NativeBatchProcessor.java
 * 所属模块: Hadoop MapReduce 原生任务扩展
 * 核心职责: 实现Java层与原生C/C++任务之间的批量数据传输和命令交互，提供统一的原生处理器入口
 */
/**
 * used to create channel, transfer data and command between Java and native
 */
@InterfaceAudience.Private
public class NativeBatchProcessor implements INativeHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativeBatchProcessor.class);

  private final String nativeHandlerName;
  private long nativeHandlerAddr;

  private boolean isInputFinished = false;

  // << Field used directly in Native, the name must NOT be changed
  private ByteBuffer rawOutputBuffer;
  private ByteBuffer rawInputBuffer;
  // >>

  private InputBuffer in;
  private OutputBuffer out;

  private CommandDispatcher commandDispatcher;
  private DataReceiver dataReceiver;

  static {
    if (NativeRuntime.isNativeLibraryLoaded()) {
      InitIDs();
    }
  }

  /**
   * 创建Native批量处理器实例，根据通道类型初始化输入输出缓冲区
   * @param nativeHandlerName 原生处理器类名
   * @param conf Hadoop配置对象
   * @param channel 数据通道类型(IN/OUT/INOUT/NONE)
   * @return 初始化完成的原生处理器实例
   * @throws IOException 初始化失败时抛出IO异常
   */
  public static INativeHandler create(String nativeHandlerName,
      Configuration conf, DataChannel channel) throws IOException {

    final int bufferSize = conf.getInt(Constants.NATIVE_PROCESSOR_BUFFER_KB,
        1024) * 1024;

    LOG.info("NativeHandler: direct buffer size: " + bufferSize);

    OutputBuffer out = null;
    InputBuffer in = null;

    // 根据通道类型创建对应缓冲区
    switch (channel) {
    case IN:
      in = new InputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      break;
    case OUT:
      out = new OutputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      break;
    case INOUT:
      in = new InputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      out = new OutputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      break;
    case NONE:
    }

    final INativeHandler handler = new NativeBatchProcessor(nativeHandlerName,
        in, out);
    handler.init(conf);
    return handler;
  }

  /**
   * 构造Native批量处理器，保存输入输出缓冲区引用
   * @param nativeHandlerName 原生处理器类名
   * @param input 输入缓冲区
   * @param output 输出缓冲区
   * @throws IOException 构造失败时抛出IO异常
   */
  protected NativeBatchProcessor(String nativeHandlerName, InputBuffer input,
      OutputBuffer output) throws IOException {
    this.nativeHandlerName = nativeHandlerName;

    if (null != input) {
      this.in = input;
      this.rawInputBuffer = input.getByteBuffer();
    }
    if (null != output) {
      this.out = output;
      this.rawOutputBuffer = output.getByteBuffer();
    }
  }

  @Override
  public void setCommandDispatcher(CommandDispatcher handler) {
    this.commandDispatcher = handler;
  }

  /**
   * 初始化原生处理器，在原生侧创建对应处理器实例并传入配置
   * @param conf Hadoop配置对象
   * @throws IOException 原生对象创建失败时抛出IO异常
   */
  @Override
  public void init(Configuration conf) throws IOException {
    this.nativeHandlerAddr = NativeRuntime
        .createNativeObject(nativeHandlerName);
    if (this.nativeHandlerAddr == 0) {
      throw new RuntimeException("Native object create failed, class: "
          + nativeHandlerName);
    }
    // 将配置序列化后传入原生侧完成处理器初始化
    setupHandler(nativeHandlerAddr, ConfigUtil.toBytes(conf));
  }

  @Override
  public synchronized void close() throws IOException {
    // 释放原生侧分配的对象内存
    if (nativeHandlerAddr != 0) {
      NativeRuntime.releaseNativeObject(nativeHandlerAddr);
      nativeHandlerAddr = 0;
    }
    // 清理Java层输入缓冲区资源
    IOUtils.cleanupWithLogger(LOG, in);
    in = null;
  }

  @Override
  public long getNativeHandler() {
    return nativeHandlerAddr;
  }

  /**
   * Java层发起命令调用，转发到原生侧处理器处理并返回结果
   * @param command 命令对象
   * @param parameter 命令参数缓冲区
   * @return 原生侧处理结果缓冲区
   * @throws IOException 调用过程出错时抛出IO异常
   */
  @Override
  public ReadWriteBuffer call(Command command, ReadWriteBuffer parameter)
      throws IOException {
    final byte[] bytes = nativeCommand(nativeHandlerAddr, command.id(),
        null == parameter ? null : parameter.getBuff());

    final ReadWriteBuffer result = new ReadWriteBuffer(bytes);
    result.setWritePoint(bytes.length);
    return result;
  }

  @Override
  public void sendData() throws IOException {
    // 通知原生侧处理当前输出缓冲区中的数据
    nativeProcessInput(nativeHandlerAddr, rawOutputBuffer.position());
    // 重置缓冲区位置，准备下一次写入
    rawOutputBuffer.position(0);
  }

  @Override
  public void finishSendData() throws IOException {
    // 缓冲区不存在或已完成输入，直接返回
    if (null == rawOutputBuffer || isInputFinished) {
      return;
    }

    // 发送剩余数据
    sendData();
    // 通知原生侧输入已完成
    nativeFinish(nativeHandlerAddr);
    isInputFinished = true;
  }

  /**
   * 原生侧发起命令调用，转发到Java层命令分发器处理并返回结果
   * @param command 命令ID
   * @param data 命令参数字节数组
   * @return Java层处理结果字节数组
   * @throws IOException 处理过程出错时抛出IO异常
   */
  private byte[] sendCommandToJava(int command, byte[] data) throws IOException {
    try {

      final Command cmd = new Command(command);
      ReadWriteBuffer param = null;

      if (null != data) {
        param = new ReadWriteBuffer();
        param.reset(data);
        param.setWritePoint(data.length);
      }

      if (null != commandDispatcher) {
        ReadWriteBuffer result = null;

        result = commandDispatcher.onCall(cmd, param);
        if (null != result) {
          return result.getBuff();
        } else {
          return null;
        }
      } else {
        return null;
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  /**
   * Called by native side, clean output buffer so native side can continue
   * processing
   * 原生侧输出完成后调用，重置输入缓冲区并触发Java层数据接收处理
   */
  private void flushOutput(int length) throws IOException {

    if (null != rawInputBuffer) {
      // 重置缓冲区位置和长度，准备读取原生侧输出
      rawInputBuffer.position(0);
      rawInputBuffer.limit(length);

      if (null != dataReceiver) {
        try {
          // 触发Java层数据接收处理
          dataReceiver.receiveData();
        } catch (IOException e) {
          e.printStackTrace();
          throw e;
        }
      }
    }
  }

  /**
   * JNI方法: 缓存JNI字段和方法ID，加速后续本地调用
   */
  private static native void InitIDs();

  /**
   * JNI方法: 初始化原生侧BatchHandler实例，传入配置信息
   */
  private native void setupHandler(long nativeHandlerAddr, byte[][] configs);

  /**
   * JNI方法: 通知原生侧处理输入缓冲区中的数据
   */
  private native void nativeProcessInput(long handler, int length);

  /**
   * JNI方法: 通知原生侧所有输入数据已发送完成
   */
  private native void nativeFinish(long handler);

  /**
   * JNI方法: Java侧向原生侧发送控制命令并返回处理结果
   */
  private native byte[] nativeCommand(long handler, int cmd, byte[] parameter);

  /**
   * JNI方法: 通知原生侧加载数据到输出缓冲区供Java层读取
   */
  private native void nativeLoadData(long handler);

  protected void finishOutput() {
  }

  @Override
  public InputBuffer getInputBuffer() {
    return this.in;
  }

  @Override
  public OutputBuffer getOutputBuffer() {
    return this.out;
  }

  @Override
  public void loadData() throws IOException {
    // 触发原生侧加载数据到缓冲区
    nativeLoadData(nativeHandlerAddr);
  }

  @Override
  public void setDataReceiver(DataReceiver handler) {
    this.dataReceiver = handler;
  }

  @Override
  public String name() {
    return nativeHandlerName;
  }
}