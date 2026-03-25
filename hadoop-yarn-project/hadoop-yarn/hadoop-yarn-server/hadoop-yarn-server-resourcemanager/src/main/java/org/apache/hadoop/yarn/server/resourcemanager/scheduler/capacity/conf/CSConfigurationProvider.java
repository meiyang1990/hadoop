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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import java.io.IOException;

/**
 * 文件提供容量调度器的配置提供者抽象接口，定义了加载容量调度器配置的统一规范，
 * 支持不同的配置存储实现（如本地文件、ZK存储等），实现配置加载逻辑的解耦。
 *
 * 容量调度器 {@link CapacityScheduler} 的配置提供者抽象接口。
 */
public interface CSConfigurationProvider {

  /**
   * 使用给定的配置初始化配置提供者。
   * @param conf 用于初始化的基础配置
   * @throws IOException 如果配置错误导致初始化失败则抛出该异常
   */
  void init(Configuration conf) throws IOException;

  /**
   * 加载容量调度器配置对象。
   * @param conf 初始引导配置
   * @return 加载完成的容量调度器配置对象
   * @throws IOException 如果获取配置失败则抛出该异常
   */
  CapacitySchedulerConfiguration loadConfiguration(Configuration conf)
      throws IOException;
}