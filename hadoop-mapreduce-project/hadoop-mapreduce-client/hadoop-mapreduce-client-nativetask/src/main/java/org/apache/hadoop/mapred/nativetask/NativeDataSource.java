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
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;

/**
 * 原生任务数据源接口，定义从MapReduce上游任务获取输入数据的规范
 * 为C++原生任务提供Java层的数据输入抽象，对接Hadoop的序列化与数据传递
 */
@InterfaceAudience.Private
public interface NativeDataSource {

  /**
   * 获取存储上游数据的输入缓冲区
   * @return 输入缓冲区对象
   */
  public InputBuffer getInputBuffer();

  /**
   * 设置数据到达监听器，当上游数据到达时触发回调处理
   * @param handler 数据接收处理器实例
   */
  void setDataReceiver(DataReceiver handler);

  /**
   * 从上游任务加载输入数据到缓冲区
   * @throws IOException 加载数据失败时抛出IO异常
   */
  public void loadData() throws IOException;

}