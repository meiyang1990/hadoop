// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认Linux容器执行器资源处理器，已废弃。
 * 提供不使用cgroups进行资源限制的默认实现，是LCEResourcesHandler接口的默认实现。
 * 该类已被标记为废弃，不再推荐使用。
 */
@Deprecated
public class DefaultLCEResourcesHandler implements LCEResourcesHandler {

  final static Logger LOG =
       LoggerFactory.getLogger(DefaultLCEResourcesHandler.class);

  private Configuration conf;
  
  public DefaultLCEResourcesHandler() {
  }
  
  /**
   * 设置节点配置信息。
   * @param conf Hadoop配置对象
   */
  public void setConf(Configuration conf) {
        this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return  conf;
  }
  
  /**
   * 初始化资源处理器，绑定到Linux容器执行器。
   * 本实现为空实现，不执行任何初始化操作。
   * @param lce Linux容器执行器实例
   */
  public void init(LinuxContainerExecutor lce) {
  }

  /*
   * LCE Resources Handler interface
   */
  
  /**
   * 容器执行前的资源准备工作。
   * 本实现为空实现，不执行任何操作。
   * @param containerId 容器ID
   * @param containerResource 容器申请的资源
   */
  public void preExecute(ContainerId containerId, Resource containerResource) {
  }
  
  /**
   * 容器执行后的资源清理工作。
   * 本实现为空实现，不执行任何操作。
   * @param containerId 容器ID
   */
  public void postExecute(ContainerId containerId) {
  }
  
  /**
   * 获取传递给容器执行器的资源参数选项。
   * 返回禁用cgroups的标识，告知LCE不使用cgroups进行资源限制。
   * @param containerId 容器ID
   * @return 资源选项字符串，标识不使用cgroups
   */
  public String getResourcesOption(ContainerId containerId) {
    return "cgroups=none";
  }


}