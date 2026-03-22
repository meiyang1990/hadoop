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
 * 与本地端交互以支持Java Combiner的接口，定义了Java端Combiner处理器需要实现的核心方法
 * 用于原生任务框架中桥接Java Combiner和本地执行环境
 */
@InterfaceAudience.Private
public interface ICombineHandler {

  /**
   * 执行Combine合并操作，处理Map输出的中间数据
   * @throws IOException 当合并过程发生IO错误时抛出
   */
  public void combine() throws IOException;

  /**
   * 获取当前Combiner处理器的唯一ID，用于标识该处理器实例
   * @return 当前处理器的ID
   */
  public long getId();

  /**
   * 关闭处理器，释放相关资源，包括缓冲区、数据拉取/推送组件等
   * @throws IOException 关闭过程发生IO错误时抛出
   */
  public void close() throws IOException;
}