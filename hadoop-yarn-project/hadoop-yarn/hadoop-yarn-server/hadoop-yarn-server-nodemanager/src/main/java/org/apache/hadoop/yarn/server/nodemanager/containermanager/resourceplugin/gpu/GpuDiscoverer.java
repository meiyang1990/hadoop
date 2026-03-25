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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourcesExceptionUtil.throwIfNecessary;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformationParser;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.PerGpuDeviceInformation;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * GPU设备发现器，负责在NodeManager节点上发现可用的NVIDIA GPU设备
 * 支持自动发现（通过nvidia-smi工具）和手动配置两种模式
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuDiscoverer extends Configured {
  public static final Logger LOG = LoggerFactory.getLogger(
      GpuDiscoverer.class);
  @VisibleForTesting
  static final String DEFAULT_BINARY_NAME = "nvidia-smi";

  // When executable path not set, try to search default dirs
  // By default search /usr/bin, /bin, and /usr/local/nvidia/bin (when
  // launched by nvidia-docker.
  private static final Set<String> DEFAULT_BINARY_SEARCH_DIRS = ImmutableSet.of(
      "/usr/bin", "/bin", "/usr/local/nvidia/bin");

  private NvidiaBinaryHelper nvidiaBinaryHelper;
  private String pathOfGpuBinary = null;
  private long discoveryTimeoutMs;
  private int discoveryMaxErrors;
  private Map<String, String> environment = new HashMap<>();

  private int numOfErrorExecutionSinceLastSucceed = 0;
  private GpuDeviceInformation lastDiscoveredGpuInformation = null;

  private List<GpuDevice> gpuDevicesFromUser;

  /** 校验配置是否已初始化，未初始化则抛出异常 */
  private void validateConfOrThrowException() throws YarnException {
    if (getConf() == null) {
      throw new YarnException("Please initialize (call initialize) before use "
          + GpuDiscoverer.class.getSimpleName());
    }
  }

  private String getErrorMessageOfScriptExecution(String msg) {
    return getFailedToExecuteScriptMessage() +
        "! Exception message: " + msg;
  }

  private String getErrorMessageOfScriptExecutionThresholdReached() {
    return getFailedToExecuteScriptMessage() + " for " +
        discoveryMaxErrors + " times, " +
        "skipping following executions!";
  }

  private String getFailedToExecuteScriptMessage() {
    return "Failed to execute " +
        GpuDeviceInformationParser.GPU_SCRIPT_REFERENCE +
        " (" + pathOfGpuBinary + ")";
  }

  private String getFailedToParseErrorMessage(String msg) {
    return "Failed to parse XML output of " +
        GpuDeviceInformationParser.GPU_SCRIPT_REFERENCE
        + "( " + pathOfGpuBinary + ")" + msg;
  }

  /**
   * 从系统获取GPU设备信息
   * 必须在initialize之后调用，仅支持Unix-like系统
   * @return GPU设备信息对象
   * @throws YarnException 当发生任何错误时抛出
   */
  public synchronized GpuDeviceInformation getGpuDeviceInformation()
      throws YarnException {
    // 检查是否已达到最大错误次数限制
    if (discoveryMaxErrors >= 0 &&
        numOfErrorExecutionSinceLastSucceed == discoveryMaxErrors) {
      String msg = getErrorMessageOfScriptExecutionThresholdReached();
      LOG.error(msg);
      throw new YarnException(msg);
    }

    try {
      // 调用NVIDIA工具获取GPU信息
      lastDiscoveredGpuInformation =
          nvidiaBinaryHelper.getGpuDeviceInformation(pathOfGpuBinary,
              discoveryTimeoutMs);
    } catch (IOException e) {
      // 执行出错，错误计数增加
      numOfErrorExecutionSinceLastSucceed++;
      String msg = getErrorMessageOfScriptExecution(e.getMessage());
      LOG.debug(msg);
      throw new YarnException(msg, e);
    } catch (YarnException e) {
      // 解析输出解析错误，错误计数增加
      numOfErrorExecutionSinceLastSucceed++;
      String msg = getFailedToParseErrorMessage(e.getMessage());
      LOG.debug(msg, e);
      throw e;
    }

    return lastDiscoveredGpuInformation;
  }

  /** 检查是否开启自动发现GPU设备模式 */
  boolean isAutoDiscoveryEnabled() {
    String allowedDevicesStr = getConf().get(
        YarnConfiguration.NM_GPU_ALLOWED_DEVICES,
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES);
    return allowedDevicesStr.equals(
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES);
  }

  /**
   * 获取YARN可使用的GPU设备列表
   * @return 可用GPU设备列表
   * @throws YarnException 发现过程中发生错误抛出
   */
  public synchronized List<GpuDevice> getGpusUsableByYarn()
      throws YarnException {
    validateConfOrThrowException();

    if (isAutoDiscoveryEnabled()) {
      // 自动发现模式，从系统信息解析GPU
      return parseGpuDevicesFromAutoDiscoveredGpuInfo();
    } else {
      // 手动配置模式，解析用户配置的GPU列表
      if (gpuDevicesFromUser == null) {
        gpuDevicesFromUser = parseGpuDevicesFromUserDefinedValues();
      }
      return gpuDevicesFromUser;
    }
  }

  /**
   * 从自动发现的GPU信息中解析出可用设备列表
   * @return 解析后的GPU设备列表
   * @throws YarnException 自动发现失败时抛出
   */
  private List<GpuDevice> parseGpuDevicesFromAutoDiscoveredGpuInfo()
          throws YarnException {
    if (lastDiscoveredGpuInformation == null) {
      String msg = YarnConfiguration.NM_GPU_ALLOWED_DEVICES + " is set to "
          + YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES
          + ", however automatically discovering "
          + "GPU information failed, please check NodeManager log for more"
          + " details, as an alternative, admin can specify "
          + YarnConfiguration.NM_GPU_ALLOWED_DEVICES
          + " manually to enable GPU isolation.";
      LOG.error(msg);
      throw new YarnException(msg);
    }

    List<GpuDevice> gpuDevices = new ArrayList<>();
    if (lastDiscoveredGpuInformation.getGpus() != null) {
      int numberOfGpus = lastDiscoveredGpuInformation.getGpus().size();
      LOG.debug("Found {} GPU devices", numberOfGpu);
      // 遍历所有GPU信息，转换为GpuDevice对象
      for (int i = 0; i < numberOfGpus; i++) {
        List<PerGpuDeviceInformation> gpuInfos =
            lastDiscoveredGpuInformation.getGpus();
        gpuDevices.add(new GpuDevice(i, gpuInfos.get(i).getMinorNumber()));
      }
    }
    return gpuDevices;
  }

  /**
   * 从用户配置中解析GPU设备列表
   * @return 解析后的GPU设备列表
   * @throws YarnException 配置格式错误或存在重复设备时抛出
   */
  private List<GpuDevice> parseGpuDevicesFromUserDefinedValues()
      throws YarnException {
    String devices = getConf().get(
        YarnConfiguration.NM_GPU_ALLOWED_DEVICES,
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES);

    // 配置为空直接抛出异常
    if (devices.trim().isEmpty()) {
      throw GpuDeviceSpecificationException.createWithEmptyValueSpecified();
    }
    List<GpuDevice> gpuDevices = Lists.newArrayList();
    // 按逗号分割多个设备
    for (String device : devices.split(",")) {
      if (device.trim().length() > 0) {
        // 按冒号分割索引和minor号
        String[] splitByColon = device.trim().split(":");
        if (splitByColon.length != 2) {
          throwIfNecessary(GpuDeviceSpecificationException
              .createWithWrongValueSpecified(device, devices), getConf());
          LOG.warn("Wrong GPU specification string {}, ignored", device);
        }

        GpuDevice gpuDevice;
        try {
          // 解析单个GPU设备
          gpuDevice = parseGpuDevice(splitByColon);
        } catch (NumberFormatException e) {
          throwIfNecessary(GpuDeviceSpecificationException
              .createWithWrongValueSpecified(device, devices, e), getConf());
          LOG.warn("Cannot parse GPU device numbers: {}", device);
          continue;
        }

        // 检查设备是否重复
        if (!gpuDevices.contains(gpuDevice)) {
          gpuDevices.add(gpuDevice);
        } else {
          throwIfNecessary(GpuDeviceSpecificationException
              .createWithDuplicateValueSpecified(device, devices), getConf());
          LOG.warn("CPU device is duplicated: {}", device);
        }
      }
    }
    LOG.info("Allowed GPU devices:" + gpuDevices);

    return gpuDevices;
  }

  /** 解析单个GPU设备，输入为[index:minorNumber]格式分割后的数组 */
  private GpuDevice parseGpuDevice(String[] splitByColon) {
    int index = Integer.parseInt(splitByColon[0]);
    int minorNumber = Integer.parseInt(splitByColon[1]);
    return new GpuDevice(index, minorNumber);
  }

  /**
   * 初始化GPU发现器，加载配置并尝试首次发现
   * @param config 配置对象
   * @param nvidiaHelper NVIDIA二进制工具帮助类
   * @throws YarnException 初始化过程出错抛出
   */
  public synchronized void initialize(Configuration config,
      NvidiaBinaryHelper nvidiaHelper) throws YarnException {
    setConf(config);
    this.nvidiaBinaryHelper = nvidiaHelper;
    if (isAutoDiscoveryEnabled()) {
      // 重置错误计数
      numOfErrorExecutionSinceLastSucceed = 0;
      // 查找nvidia-smi二进制文件路径
      lookUpAutoDiscoveryBinary(config);

      // 首次尝试发现GPU信息，打印日志
      try {
        LOG.info("Trying to discover GPU information ...");
        GpuDeviceInformation info = getGpuDeviceInformation();
        LOG.info("Discovered GPU information: " + info.toString());
      } catch (YarnException e) {
        String msg =
                "Failed to discover GPU information from system, exception message:"
                        + e.getMessage() + " continue...";
        LOG.warn(msg);
      }
    }
  }

  /**
   * 查找nvidia-smi二进制文件路径，处理用户配置和默认搜索
   * @param config 配置对象
   * @throws YarnException 找不到二进制文件时抛出
   */
  private void lookUpAutoDiscoveryBinary(Configuration config)
      throws YarnException {
    String configuredBinaryPath = config.get(
        YarnConfiguration.NM_GPU_PATH_TO_EXEC, DEFAULT_BINARY_NAME);
    if (configuredBinaryPath.isEmpty()) {
      configuredBinaryPath = DEFAULT_BINARY_NAME;
    }

    File binaryPath;
    File configuredBinaryFile = new File(configuredBinaryPath);
    if (!configuredBinaryFile.exists()) {
      // 用户配置路径不存在，去默认目录搜索
      binaryPath = lookupBinaryInDefaultDirs();
    } else if (configuredBinaryFile.isDirectory()) {
      // 用户配置是目录，在目录下查找nvidia-smi
      binaryPath = handleConfiguredBinaryPathIsDirectory(configuredBinaryFile);
    } else {
      // 用户配置是文件，直接使用，检查文件名是否正确
      binaryPath = configuredBinaryFile;
      // If path exists but file name is incorrect don't execute the file
      String fileName = binaryPath.getName();
      if (!DEFAULT_BINARY_NAME.equals(fileName)) {
        String msg = String.format("Please check the configuration value of"
             +" %s. It should point to an %s binary, which is now %s",
             YarnConfiguration.NM_GPU_PATH_TO_EXEC,
             DEFAULT_BINARY_NAME,
             fileName);
        throwIfNecessary(new YarnException(msg), config);
        LOG.warn(msg);
      }
    }

    // 保存绝对路径
    pathOfGpuBinary = binaryPath.getAbsolutePath();

    // 读取发现超时配置
    discoveryTimeoutMs = config.getTimeDuration(
        YarnConfiguration.NM_GPU_DISCOVERY_TIMEOUT,
        YarnConfiguration.NM_GPU_DISCOVERY_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    // 读取最大错误次数配置
    discoveryMaxErrors = config.getInt(
        YarnConfiguration.NM_GPU_DISCOVERY_MAX_ERRORS,
        YarnConfiguration.NM_GPU_DISCOVERY_MAX_ERRORS_DEFAULT);

  }

  /**
   * 处理用户配置为目录的情况，在目录下查找nvidia-smi
   * @param configuredBinaryFile 用户配置的目录
   * @return 找到的nvidia-smi文件对象
   * @throws YarnException 目录下找不到nvidia-smi抛出
   */
  private File handleConfiguredBinaryPathIsDirectory(File configuredBinaryFile)
      throws YarnException {
    File binaryPath = new File(configuredBinaryFile, DEFAULT_BINARY_NAME);
    if (!binaryPath.exists()) {
      throw new YarnException("Failed to find GPU discovery executable, " +
          "please double check "+ YarnConfiguration.NM_GPU_PATH_TO_EXEC +
          " setting. The setting points to a directory but " +
          "no file found in the directory with name:" + DEFAULT_BINARY_NAME);
    } else {
      LOG.warn("Specified path is a directory, use " + DEFAULT_BINARY_NAME
          + " under the directory, updated path-to-executable:"
          + binaryPath.getAbsolutePath());
    }
    return binaryPath;
  }

  /**
   * 在默认搜索目录中查找nvidia-smi
   * @return 找到的文件对象
   * @throws YarnException 所有默认目录都找不到抛出
   */
  private File lookupBinaryInDefaultDirs() throws YarnException {
    final File lookedUpBinary = lookupBinaryInDefaultDirsInternal();
    if (lookedUpBinary == null) {
      throw new YarnException("Failed to find GPU discovery executable, " +
          "please double check " + YarnConfiguration.NM_GPU_PATH_TO_EXEC +
          " setting. Also tried to find the executable " +
          "in the default directories: " + DEFAULT_BINARY_SEARCH_DIRS);
    }
    return lookedUpBinary;
  }

  /** 内部方法：遍历默认目录查找nvidia-smi */
  private File lookupBinaryInDefaultDirsInternal() {
    Set<String> triedBinaryPaths = Sets.newHashSet();
    for (String dir : DEFAULT_BINARY_SEARCH_DIRS) {
      File binaryPath = new File(dir, DEFAULT_BINARY_NAME);
      if (binaryPath.exists()) {
        return binaryPath;
      } else {
        triedBinaryPaths.add(binaryPath.getAbsolutePath());
      }
    }
    LOG.warn("Failed to locate GPU device discovery binary, tried paths: "
        + triedBinaryPaths + "! Please double check the value of config "
        + YarnConfiguration.NM_GPU_PATH_TO_EXEC +
        ". Using default binary: " + DEFAULT_BINARY_NAME);

    return null;
  }

  @VisibleForTesting
  Map<String, String> getEnvironmentToRunCommand() {
    return environment;
  }

  @VisibleForTesting
  String getPathOfGpuBinary() {
    return pathOfGpuBinary;
  }
}