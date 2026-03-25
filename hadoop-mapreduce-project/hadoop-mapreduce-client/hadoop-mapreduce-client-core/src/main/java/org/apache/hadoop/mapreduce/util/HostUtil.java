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

package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * MapReduce任务主机地址处理工具类，提供任务日志URL构造、Tracker名称转换等功能
 * 用于处理MapReduce任务执行过程中与主机地址相关的通用操作
 */
@Private
@Unstable
public class HostUtil {

  /**
   * 构造任务日志的完整访问URL
   * @param scheme URL协议头(http/https)
   * @param taskTrackerHostName TaskTracker主机名
   * @param httpPort TaskTracker HTTP服务端口
   * @param taskAttemptID 任务尝试ID
   * @return 拼接完成的任务日志访问URL
   */
  public static String getTaskLogUrl(String scheme, String taskTrackerHostName,
    String httpPort, String taskAttemptID) {
    return (scheme + taskTrackerHostName + ":" +
        httpPort + "/tasklog?attemptid=" + taskAttemptID);
  }

  /**
   * 为保持二进制兼容性保留的过期方法，运行时调用一定会抛出异常
   * 仅用于兼容Hive 0.13版本，不应该在实际运行时被调用
   * @deprecated 使用{@link #getTaskLogUrl(String, String, String, String)}替代
   * @param taskTrackerHostName TaskTracker主机名
   * @param httpPort TaskTracker HTTP服务端口
   * @param taskAttemptID 任务尝试ID
   * @return 永远不会返回正常结果
   */
  @Deprecated
  public static String getTaskLogUrl(String taskTrackerHostName,
                                     String httpPort, String taskAttemptID) {
    throw new RuntimeException(
        "This method is not supposed to be called at runtime. " +
        "Use HostUtil.getTaskLogUrl(String, String, String, String) instead.");
  }

  /**
   * 将Tracker名称格式转换为纯主机名格式
   * 移除Tracker名称前缀和端口部分，提取出纯净的主机名
   * @param trackerName 原始Tracker名称，格式一般为tracker_<host>:<port>
   * @return 提取后的纯净主机名
   */
  public static String convertTrackerNameToHostName(String trackerName) {
    // 查找冒号位置分割主机名和端口
    int indexOfColon = trackerName.indexOf(":");
    // 截取冒号前的部分作为主机名部分，无冒号则使用完整字符串
    String trackerHostName = (indexOfColon == -1) ? 
      trackerName : 
      trackerName.substring(0, indexOfColon);
    // 移除"tracker_"前缀得到最终主机名
    return trackerHostName.substring("tracker_".length());
  }

}