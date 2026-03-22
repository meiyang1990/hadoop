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

package org.apache.hadoop.mapreduce.v2.app.rm;

/**
 * RM心跳处理器接口，定义MapReduce应用程序与ResourceManager心跳交互的回调能力
 * 用于获取最后一次心跳时间，以及注册下次心跳时需要执行的任务
 */
public interface RMHeartbeatHandler {
  /**
   * 获取上次向ResourceManager发送心跳的时间戳
   * @return 上次心跳的时间戳（毫秒）
   */
  long getLastHeartbeatTime();

  /**
   * 注册下次心跳发送后需要执行的回调任务
   * @param callback 要在下次心跳时执行的任务
   */
  void runOnNextHeartbeat(Runnable callback);
}