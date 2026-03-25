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

package org.apache.hadoop.yarn.server.nodemanager.health;

/**
 * 服务健康状态报告接口，定义了获取节点健康状态相关信息的标准方法
 * 由NodeManager健康检查服务使用，提供三方面核心健康信息：
 * <ul>
 * <li>服务当前是否健康 ({@link #isHealthy()})</li>
 * <li>健康检查结果报告 ({@link #getHealthReport()})</li>
 * <li>最近一次健康检查的时间戳 ({@link #getLastHealthReportTime()})</li>
 * </ul>
 *
 * 实现该接口的类会被 {@link NodeHealthCheckerService} 集成调用
 *
 * 不推荐开发者新增Java自定义实现，更建议通过脚本实现健康检查，
 * 然后使用 {@link NodeHealthScriptRunner} 处理脚本执行与结果解析
 *
 * @see TimedHealthReporterService
 * @see org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService
 */
public interface HealthReporter {

  /**
   * 获取当前节点是否健康
   *
   * @return true 节点健康，false 节点不健康
   */
  boolean isHealthy();

  /**
   * 获取健康检查结果报告，节点健康时返回空字符串
   *
   * @return 健康检查输出报告
   */
  String getHealthReport();

  /**
   * 获取最近一次执行健康检查的时间戳
   *
   * @return 最近一次健康检查的时间戳
   */
  long getLastHealthReportTime();
}