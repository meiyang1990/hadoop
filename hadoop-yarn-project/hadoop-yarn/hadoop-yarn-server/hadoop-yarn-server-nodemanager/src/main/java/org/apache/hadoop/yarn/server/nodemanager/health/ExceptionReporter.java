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
 * NodeManager健康检查的异常报告器，实现HealthReporter接口，
 * 用于报告NodeManager是否发生了致命异常，标记节点健康状态。
 * 
 * 该类被{@link org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl}
 * 的reportException方法调用，用于向ResourceManager上报节点异常状态
 */
public class ExceptionReporter implements HealthReporter {
  private Exception nodeHealthException;
  private long nodeHealthExceptionReportTime;

  ExceptionReporter() {
    this.nodeHealthException = null;
    this.nodeHealthExceptionReportTime = 0;
  }

  /**
   * 检查当前NodeManager节点是否健康
   * @return 无异常时返回true，发生致命异常时返回false
   */
  @Override
  public synchronized boolean isHealthy() {
    return nodeHealthException == null;
  }

  /**
   * 获取当前节点的健康异常报告
   * @return 无异常返回null，否则返回异常的信息字符串
   */
  @Override
  public synchronized String getHealthReport() {
    return nodeHealthException == null ? null :
        nodeHealthException.getMessage();
  }

  /**
   * 获取最后一次异常报告的时间戳
   * @return 异常报告时间戳（毫秒）
   */
  @Override
  public synchronized long getLastHealthReportTime() {
    return nodeHealthExceptionReportTime;
  }

  /**
   * 报告致命异常，将当前节点标记为不健康
   * @param ex 导致节点不健康的异常
   */
  public synchronized void reportException(Exception ex) {
    nodeHealthException = ex;
    nodeHealthExceptionReportTime = System.currentTimeMillis();
  }
}