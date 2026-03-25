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

package org.apache.hadoop.hdfs.server.sps;

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.IOException;

/**
 * 外部存储策略满足器(External SPS)错误注入器，用于单元测试中注入异常模拟故障场景
 * 为测试异常处理逻辑提供可注入的故障点
 */
/**
 * Used to inject certain faults for testing.
 */
public class ExternalSPSFaultInjector {
  @VisibleForTesting
  // 单例实例
  private static ExternalSPSFaultInjector instance =
      new ExternalSPSFaultInjector();

  /**
   * 获取错误注入器单例实例
   * @return 错误注入器单例
   */
  @VisibleForTesting
  public static ExternalSPSFaultInjector getInstance() {
    return instance;
  }

  /**
   * 设置错误注入器实例，用于注入自定义的mock实现
   * @param instance 要设置的错误注入器实例
   */
  @VisibleForTesting
  public static void setInstance(ExternalSPSFaultInjector instance) {
    ExternalSPSFaultInjector.instance = instance;
  }

  /**
   * 模拟抛出异常，可根据重试次数定制故障行为
   * @param retry 当前重试次数
   * @throws IOException 模拟抛出的IO异常
   */
  @VisibleForTesting
  public void mockAnException(int retry) throws IOException {
  }
}