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

package org.apache.hadoop.hdfs.server.datanode.checker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 可检测对象接口，为DataNode健康检查体系定义可检测对象规范，
 * 可通过调用check方法检测对象健康状态。
 * 例如一个可检测对象可以代表一块硬件资源，用于后台周期检查其健康状态。
 * 
 * @param <K> 检测上下文参数类型
 * @param <V> 检测结果类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface Checkable<K, V> {

  /**
   * 执行健康状态检查，本方法可能根据目标资源状态无限阻塞。
   *
   * @param context 探测操作的上下文，具体实现可允许为null
   *
   * @return 检查操作的结果
   *
   * @throws Exception 检查过程中发生异常，代表本次检查失败
   */
  V check(K context) throws Exception;
}