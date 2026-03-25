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
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

/**
 * 命令分发器接口，接收来自上游的命令并执行对应操作
 * 用于Native Task框架中Java层与Native层之间的命令调用分发
 */
@InterfaceAudience.Private
public interface CommandDispatcher {
  /**
   * 处理调用命令，执行对应操作并返回结果
   * @param command 待处理的命令对象
   * @param parameter 命令参数缓冲区
   * @return 命令处理结果缓冲区
   * @throws IOException 处理过程中发生IO异常时抛出
   */
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException;
}