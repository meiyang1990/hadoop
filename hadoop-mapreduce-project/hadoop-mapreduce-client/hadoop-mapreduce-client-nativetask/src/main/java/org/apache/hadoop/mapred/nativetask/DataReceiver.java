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

/**
 * 数据接收接口，用于拉取到达的新数据，在原生任务框架中处理跨进程/跨模块数据传输
 * 典型实现为{@link org.apache.hadoop.mapred.nativetask.handlers.BufferPuller}
 * 用于MapReduce原生任务的shuffle数据拉取场景
 */
@InterfaceAudience.Private
public interface DataReceiver {

  /**
   * 向接收端发送数据已到达的信号，实际数据通过其他通道传输
   * @return 是否成功接收数据
   * @throws IOException 传输过程中发生IO异常
   */
  public boolean receiveData() throws IOException;
}