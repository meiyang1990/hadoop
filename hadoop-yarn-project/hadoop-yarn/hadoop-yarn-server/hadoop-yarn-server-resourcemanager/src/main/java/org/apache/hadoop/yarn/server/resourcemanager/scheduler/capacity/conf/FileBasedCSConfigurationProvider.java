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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import java.io.IOException;
import java.io.InputStream;

/**
 * 基于本地capacity-scheduler.xml文件的容量调度器配置提供器，
 * 从本地配置文件加载容量调度器的队列配置。
 */
public class FileBasedCSConfigurationProvider implements
    CSConfigurationProvider {

  private RMContext rmContext;

  /**
   * 构造基于文件的容量调度器配置提供器，绑定RM上下文
   * @param rmContext ResourceManager上下文对象
   */
  public FileBasedCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration conf) {}

  /**
   * 从本地配置文件加载容量调度器配置
   * @param conf 基础YARN配置对象
   * @return 加载完成的容量调度器配置对象
   * @throws IOException 加载配置过程中发生IO异常时抛出
   */
  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration conf)
      throws IOException {
    try {
      // 从RM配置提供器获取容量调度器配置文件输入流
      InputStream csInputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.CS_CONFIGURATION_FILE);
      // 如果找到配置文件，将其添加到配置中并返回实例
      if (csInputStream != null) {
        conf.addResource(csInputStream);
        return new CapacitySchedulerConfiguration(conf, false);
      }
      // 未找到配置文件，使用默认配置创建实例
      return new CapacitySchedulerConfiguration(conf, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}