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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * VolumeScanner和BlockScanner测试的回调注入工具类
 * 用于在单元测试中注入自定义回调钩子，生产环境中所有方法均为空实现
 * 
 * 核心职责：为DataNode卷扫描器提供测试回调注入点，实现测试逻辑与核心业务逻辑解耦
 * 设计目的：不修改生产代码即可在测试中拦截扫描过程，实现对扫描逻辑的观测与控制
 */
@VisibleForTesting
@InterfaceAudience.Private
public class VolumeScannerCBInjector {
  private static VolumeScannerCBInjector instance =
      new VolumeScannerCBInjector();

  /**
   * 获取回调注入器单例实例
   * @return 回调注入器实例
   */
  public static VolumeScannerCBInjector get() {
    return instance;
  }

  /**
   * 设置自定义回调注入器实例，用于测试时替换默认实例
   * @param injector 自定义回调注入器实例
   */
  public static void set(VolumeScannerCBInjector injector) {
    instance = injector;
  }

  /**
   * 保存块迭代器任务前的回调钩子
   * @param volumeScanner 当前执行的VolumeScanner实例
   */
  public void preSavingBlockIteratorTask(final VolumeScanner volumeScanner) {
  }

  /**
   * 关闭VolumeScanner时的回调钩子
   * @param volumeScanner 当前执行的VolumeScanner实例
   */
  public void shutdownCallBack(final VolumeScanner volumeScanner) {
  }

  /**
   * VolumeScanner终止时的回调钩子
   * @param volumeScanner 当前执行的VolumeScanner实例
   */
  public void terminationCallBack(final VolumeScanner volumeScanner) {
  }
}