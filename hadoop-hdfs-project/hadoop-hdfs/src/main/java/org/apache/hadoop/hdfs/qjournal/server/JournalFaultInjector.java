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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：QJournal节点日志故障注入类，用于在QuorumJournalManager测试中注入故障，生产环境中所有方法均为空实现不生效。
 * 该类为测试专用，提供了在Paxos数据持久化关键节点注入异常的能力，用于验证系统容错性和故障恢复能力。
 */
@VisibleForTesting
@InterfaceAudience.Private
public class JournalFaultInjector {
  /** 全局单例实例，生产环境使用默认空实现 */
  public static JournalFaultInjector instance = new JournalFaultInjector();

  /**
   * 获取故障注入器单例实例
   * @return 返回故障注入器实例，生产环境返回默认空实现
   */
  public static JournalFaultInjector get() {
    return instance;
  }

  /**
   * 在持久化Paxos数据之前执行的故障注入点，测试代码可覆盖实现注入异常
   * @throws IOException 测试时可抛出该异常模拟持久化前失败
   */
  public void beforePersistPaxosData() throws IOException {}
  /**
   * 在持久化Paxos数据之后执行的故障注入点，测试代码可覆盖实现注入异常
   * @throws IOException 测试时可抛出该异常模拟持久化后失败
   */
  public void afterPersistPaxosData() throws IOException {}
}