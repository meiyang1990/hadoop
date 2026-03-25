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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

/**
 * FPGA资源插件实现类，为NodeManager提供FPGA异构资源管理能力
 * 负责初始化FPGA相关组件、创建资源处理器和节点资源更新处理器，对接不同厂商的FPGA设备
 */
public class FpgaResourcePlugin implements ResourcePlugin {
  private static final Logger LOG = LoggerFactory.
      getLogger(FpgaResourcePlugin.class);

  private ResourceHandler fpgaResourceHandler = null;

  private AbstractFpgaVendorPlugin vendorPlugin = null;
  private FpgaNodeResourceUpdateHandler fpgaNodeResourceUpdateHandler = null;
  private FpgaDiscoverer fpgaDiscoverer;

  /**
   * 根据配置创建对应厂商的FPGA插件实例
   * @param conf YARN配置对象
   * @return 初始化完成的FPGA厂商插件实例
   */
  private AbstractFpgaVendorPlugin createFpgaVendorPlugin(Configuration conf) {
    String vendorPluginClass = conf.get(YarnConfiguration.NM_FPGA_VENDOR_PLUGIN,
        YarnConfiguration.DEFAULT_NM_FPGA_VENDOR_PLUGIN);
    LOG.info("Using FPGA vendor plugin: " + vendorPluginClass);
    try {
      Class<?> schedulerClazz = Class.forName(vendorPluginClass);
      if (AbstractFpgaVendorPlugin.class.isAssignableFrom(schedulerClazz)) {
        return (AbstractFpgaVendorPlugin) ReflectionUtils.newInstance(schedulerClazz,
            conf);
      } else {
        throw new YarnRuntimeException("Class: " + vendorPluginClass
            + " not instance of " + AbstractFpgaVendorPlugin.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate FPGA vendor plugin: "
          + vendorPluginClass, e);
    }
  }

  @Override
  public void initialize(Context context) throws YarnException {
    // 从配置加载厂商插件
    this.vendorPlugin = createFpgaVendorPlugin(context.getConf());
    // 初始化FPGA设备发现器
    fpgaDiscoverer = new FpgaDiscoverer();
    fpgaDiscoverer.setResourceHanderPlugin(vendorPlugin);
    fpgaDiscoverer.initialize(context.getConf());
    // 创建节点资源更新处理器
    fpgaNodeResourceUpdateHandler =
        new FpgaNodeResourceUpdateHandler(fpgaDiscoverer);
  }

  @Override
  public ResourceHandler createResourceHandler(
      Context nmContext, CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    // 单例模式创建FPGA资源处理器
    if (fpgaResourceHandler == null) {
      fpgaResourceHandler = new FpgaResourceHandlerImpl(nmContext,
          cGroupsHandler, privilegedOperationExecutor, vendorPlugin,
          fpgaDiscoverer);
    }
    return fpgaResourceHandler;
  }

  @Override
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    return fpgaNodeResourceUpdateHandler;
  }

  @Override
  public void cleanup() throws YarnException {

  }

  @Override
  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return null;
  }

  @Override
  public NMResourceInfo getNMResourceInfo() throws YarnException {
    return null;
  }

  @Override
  public String toString() {
    return FpgaResourcePlugin.class.getName();
  }
}