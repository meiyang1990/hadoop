// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourceManager运行信息的JMX MBean实现，提供通过JMX暴露RM状态信息的能力。
 */
public class RMInfo implements RMInfoMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(RMNMInfo.class);
  private ResourceManager resourceManager;
  private ObjectName rmStatusBeanName;

  /**
   * 构造RMInfo对象，持有ResourceManager实例引用。
   *
   * @param resourceManager ResourceManager实例
   */
  RMInfo(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  /**
   * 向MBean服务器注册当前RMInfo MBean，暴露RM运行信息供JMX监控。
   */
  public void register() {
    StandardMBean bean;
    try {
      // 创建标准MBean实例，使用RMInfoMXBean接口
      bean = new StandardMBean(this, RMInfoMXBean.class);
      // 注册MBean到MBean服务器，存储注册后的对象名
      rmStatusBeanName = MBeans.register("ResourceManager", "RMInfo", bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering RMInfo MBean", e);
    }
    LOG.info("Registered RMInfo MBean");
  }

  /**
   * 从MBean服务器注销当前RMInfo MBean。
   */
  public void unregister() {
    if (rmStatusBeanName != null) {
      MBeans.unregister(rmStatusBeanName);
    }
  }

  @Override 
  public String getState() {
    // 获取并返回当前ResourceManager的高可用状态
    return this.resourceManager.getRMContext().getHAServiceState().toString();
  }

  @Override 
  public String getHostAndPort() {
    // 获取并返回当前ResourceManager绑定的服务地址
    return NetUtils.getHostPortString(ResourceManager.getBindAddress(
        this.resourceManager.getRMContext().getYarnConfiguration()));
  }

  @Override 
  public boolean isSecurityEnabled() {
    // 返回集群安全认证是否启用
    return UserGroupInformation.isSecurityEnabled();
  }
}