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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;

import java.util.Set;

/**
 * 设备资源更新实现，对接NodeStatusUpdater更新节点设备资源信息
 * 属于YARN NodeManager设备框架，负责将设备插件发现的可用设备资源更新到节点资源中
 */
public class DeviceResourceUpdaterImpl extends NodeResourceUpdaterPlugin {

  final static Logger LOG = LoggerFactory.
      getLogger(DeviceResourceUpdaterImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;

  /**
   * 构造设备资源更新器
   * @param resourceName 资源类型名称
   * @param devicePlugin 对应设备插件实例
   */
  public DeviceResourceUpdaterImpl(String resourceName,
      DevicePlugin devicePlugin) {
    this.devicePlugin = devicePlugin;
    this.resourceName = resourceName;
  }

  @Override
  /**
   * 更新节点已配置的设备资源总量
   * @param res 节点资源对象，用于更新资源值
   * @throws YarnException 设备发现异常时抛出
   */
  public void updateConfiguredResource(Resource res)
      throws YarnException {
    LOG.info(resourceName + " plugin update resource ");
    Set<Device> devices = null;
    try {
      // 通过设备插件获取当前节点所有可用设备
      devices = devicePlugin.getDevices();
    } catch (Exception e) {
      throw new YarnException("Exception thrown from plugin's getDevices"
          + e.getMessage());
    }
    if (null == devices) {
      LOG.warn(resourceName
          + " plugin failed to discover resource ( null value got).");
      return;
    }
    // 将可用设备数量设置到节点资源中，供RM调度使用
    res.setResourceValue(resourceName, devices.size());
  }

}