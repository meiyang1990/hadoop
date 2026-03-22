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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务数据加载器接口，定义按需加载输入数据的通用规范
 * 为MapReduce原生任务框架提供可扩展的数据加载能力
 */
@InterfaceAudience.Private
public interface IDataLoader {

  /**
   * 加载数据到缓冲区，供原生任务处理
   * @return 本次加载的数据大小（字节数）
   * @throws IOException 加载过程中发生IO异常
   */
  public int load() throws IOException;

  /**
   * 关闭数据加载器，释放占用的资源
   * @throws IOException 关闭过程中发生IO异常
   */
  public void close() throws IOException;
}