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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.nativetask.buffer.OutputBuffer;

/**
 * 原生任务数据输出目标接口，定义了向下游发送处理后数据的统一规范
 * 供Native MapReduce任务在C++层面输出数据到Java层后续处理使用
 */
@InterfaceAudience.Private
public interface NativeDataTarget {

  /**
   * 发送数据就绪信号，通知下游数据已经写入输出缓冲区，可以读取处理
   * @throws IOException 发送过程中发生I/O异常时抛出
   */
  public void sendData() throws IOException;

  /**
   * 发送数据发送完成信号，通知下游当前阶段所有数据已经发送完毕，没有更多数据
   * @throws IOException 发送过程中发生I/O异常时抛出
   */
  public void finishSendData() throws IOException;

  /**
   * 获取当前数据目标对应的输出缓冲区，用于写入待发送数据
   * @return 可写入的输出缓冲区对象
   */
  public OutputBuffer getOutputBuffer();

}