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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.AoclOutputBasedDiscoveryStrategy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.FPGADiscoveryStrategy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.ScriptBasedFPGADiscoveryStrategy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.SettingsBasedFPGADiscoveryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * YARN NodeManager FPGA设备发现器，负责根据不同配置策略发现当前节点上可用的FPGA设备
 * 支持静态配置、自定义发现脚本、默认AOCL工具发现三种发现策略，最终根据用户配置过滤出允许使用的设备列表
 */
public class FpgaDiscoverer extends Configured {
  private static final Logger LOG = LoggerFactory.getLogger(
      FpgaDiscoverer.class);

  // FPGA厂商插件实例，提供厂商特定的设备类型与诊断能力
  private AbstractFpgaVendorPlugin plugin = null;
  // 当前节点已发现并配置允许使用的FPGA设备列表
  private List<FpgaDevice> currentFpgaInfo = null;

  // 发现脚本执行器，默认使用本地shell执行发现脚本
  private Function<String, Optional<String>> scriptRunner = this::runScript;

  // shell命令执行超时时间，单位毫秒
  public static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;

  @VisibleForTesting
  void setScriptRunner(Function<String, Optional<String>> scriptRunner) {
    this.scriptRunner = scriptRunner;
  }

  public List<FpgaDevice> getCurrentFpgaInfo() {
    return currentFpgaInfo;
  }

  public void setResourceHanderPlugin(AbstractFpgaVendorPlugin vendorPlugin) {
    this.plugin = vendorPlugin;
  }

  /**
   * 使用厂商插件执行FPGA设备健康诊断
   * @return 诊断是否通过
   */
  public boolean diagnose() {
    return this.plugin.diagnose(MAX_EXEC_TIMEOUT_MS);
  }

  /**
   * 初始化FPGA发现器，加载配置并初始化厂商插件，执行设备诊断
   * @param config YARN配置
   * @throws YarnException 初始化异常
   */
  public void initialize(Configuration config) throws YarnException {
    setConf(config);
    this.plugin.initPlugin(config);
    // 尝试诊断FPGA设备状态
    LOG.info("Trying to diagnose FPGA information ...");
    if (!diagnose()) {
      LOG.warn("Failed to pass FPGA devices diagnose");
    }
  }

  /**
   * Get available devices minor numbers from toolchain or static configuration.
   *
   * @return the list of FPGA devices
   * @throws ResourceHandlerException if there's any error during discovery
   **/
  public List<FpgaDevice> discover()
      throws ResourceHandlerException {
    List<FpgaDevice> list;
    // 获取用户配置的允许使用的FPGA设备列表
    String allowed = getConf().get(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES);

    // 获取静态配置的可用FPGA设备
    String availableDevices = getConf().get(
        YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES);
    // 获取自定义发现脚本路径
    String discoveryScript = getConf().get(
        YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT);

    FPGADiscoveryStrategy discoveryStrategy;
    // 优先级1：配置了静态可用设备列表，使用静态配置发现策略
    if (availableDevices != null) {
      discoveryStrategy =
          new SettingsBasedFPGADiscoveryStrategy(
              plugin.getFpgaType(), availableDevices);
    } 
    // 优先级2：配置了发现脚本，使用脚本发现策略
    else if (discoveryScript != null) {
      discoveryStrategy =
          new ScriptBasedFPGADiscoveryStrategy(
              plugin.getFpgaType(), scriptRunner, discoveryScript);
    } 
    // 默认：使用英特尔AOCL工具输出发现策略
    else {
      discoveryStrategy = new AoclOutputBasedDiscoveryStrategy(plugin);
    }

    // 执行设备发现
    list = discoveryStrategy.discover();

    // 根据允许设备配置过滤最终可用设备
    if (allowed == null || allowed.equalsIgnoreCase(
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES)) {
      // 自动发现模式，使用全部发现到的设备
      currentFpgaInfo = ImmutableList.copyOf(list);
      return list;
    } else if (allowed.matches("(\\d,)*\\d")){
      // 用户指定了具体设备次设备号，解析并过滤
      Set<String> minors = Sets.newHashSet(allowed.split(","));

      // 按用户配置过滤设备列表
      list = list
        .stream()
        .filter(dev -> minors.contains(String.valueOf(dev.getMinor())))
        .collect(Collectors.toList());

      currentFpgaInfo = ImmutableList.copyOf(list);

      // 用户配置的设备数量和实际发现的不一致，输出警告但继续执行
      if (list.size() != minors.size()) {
        LOG.warn("We continue although there're mistakes in user's configuration " +
            YarnConfiguration.NM_FPGA_ALLOWED_DEVICES +
            "user configured:" + allowed + ", while the real:" + list.toString());
      }
    } else {
      // 配置格式非法，抛出异常
      throw new ResourceHandlerException("Invalid value configured for " +
          YarnConfiguration.NM_FPGA_ALLOWED_DEVICES + ":\"" + allowed + "\"");
    }

    return list;
  }

  /**
   * 执行指定路径的发现脚本，返回脚本标准输出
   * @param path 脚本文件路径
   * @return 脚本输出结果，执行失败返回空Optional
   */
  private Optional<String> runScript(String path) {
    if (path == null || path.trim().isEmpty()) {
      LOG.error("Undefined script");
      return Optional.empty();
    }

    File f = new File(path);
    if (!f.exists()) {
      LOG.error("Script does not exist");
      return Optional.empty();
    }

    if (!FileUtil.canExecute(f)) {
      LOG.error("Script is not executable");
      return Optional.empty();
    }

    // 构建shell命令执行器，设置超时时间
    ShellCommandExecutor shell = new ShellCommandExecutor(
        new String[] {path},
        null,
        null,
        MAX_EXEC_TIMEOUT_MS);
    try {
      shell.execute();
      String output = shell.getOutput();
      return Optional.of(output);
    } catch (IOException e) {
      LOG.error("Cannot execute script", e);
      return Optional.empty();
    }
  }
}