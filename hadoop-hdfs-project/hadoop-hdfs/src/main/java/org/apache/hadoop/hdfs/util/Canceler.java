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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * HDFS跨线程取消操作工具类，提供线程安全的取消标记机制。
 * 允许一个线程标记操作需要取消，另一个线程轮询检查取消状态，
 * 常用于长时间运行的异步操作的中断处理。
 */
@InterfaceAudience.Private
public class Canceler {
  /**
   * 存储取消原因，如果为null表示操作未被取消，非null表示操作已取消，值为取消原因
   * 使用volatile保证多线程下的可见性
   */
  private volatile String cancelReason = null;
  
  /**
   * 请求取消当前正在运行的操作，非阻塞方法，不会等待取消完成。
   * @param reason 请求取消的原因描述
   */
  public void cancel(String reason) {
    this.cancelReason = reason;
  }

  /**
   * 检查当前操作是否已经被请求取消
   * @return true表示操作已被取消，false表示未取消
   */
  public boolean isCancelled() {
    return cancelReason != null;
  }
  
  /**
   * 获取操作被取消的原因描述
   * @return 取消原因字符串，未取消时返回null
   */
  public String getCancellationReason() {
    return cancelReason;
  }
}