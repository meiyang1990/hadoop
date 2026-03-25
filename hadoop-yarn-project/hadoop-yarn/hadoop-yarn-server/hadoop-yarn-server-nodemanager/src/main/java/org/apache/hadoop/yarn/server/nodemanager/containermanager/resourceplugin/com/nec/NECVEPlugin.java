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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nec;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * NEC Vector Engine（向量引擎）设备插件，支持YARN对NEC VE设备的发现和调度管理。
 * 实现了DevicePlugin和DevicePluginScheduler接口，提供设备发现和分配能力。
 *
 */
public class NECVEPlugin implements DevicePlugin, DevicePluginScheduler {
  /** 环境变量名：Hadoop公共主目录 */
  private static final String HADOOP_COMMON_HOME = "HADOOP_COMMON_HOME";
  /** 环境变量名：设备发现脚本路径 */
  private static final String ENV_SCRIPT_PATH = "NEC_VE_GET_SCRIPT_PATH";
  /** 环境变量名：设备发现脚本名称 */
  private static final String ENV_SCRIPT_NAME = "NEC_VE_GET_SCRIPT_NAME";
  /** 环境变量名：是否使用udev进行设备发现 */
  private static final String ENV_USE_UDEV = "NEC_USE_UDEV";
  /** 默认设备发现脚本名称 */
  private static final String DEFAULT_SCRIPT_NAME = "nec-ve-get.py";
  private static final Logger LOG = LoggerFactory.getLogger(NECVEPlugin.class);
  /** 默认脚本搜索路径数组 */
  private static final String[] DEFAULT_BINARY_SEARCH_DIRS = new String[]{
      "/usr/bin", "/bin", "/opt/nec/ve/bin"};

  private String binaryPath;
  private boolean useUdev;
  private VEDeviceDiscoverer discoverer;

  /** 命令执行器工厂，用于执行外部设备发现脚本，支持测试注入 */
  private Function<String[], CommandExecutor>
      commandExecutorProvider = this::createCommandExecutor;

  /**
   * 默认构造函数，使用系统环境和默认配置初始化插件。
   * @throws ResourceHandlerException 初始化失败时抛出异常
   */
  public NECVEPlugin() throws ResourceHandlerException {
    this(System::getenv, DEFAULT_BINARY_SEARCH_DIRS, new UdevUtil());
  }

  /**
   * 测试用构造函数，支持注入依赖进行单元测试。
   * @param envProvider 环境变量获取函数
   * @param scriptPaths 脚本搜索路径数组
   * @param udev udev工具实例
   * @throws ResourceHandlerException 初始化失败时抛出异常
   */
  @VisibleForTesting
  NECVEPlugin(Function<String, String> envProvider, String[] scriptPaths,
      UdevUtil udev) throws ResourceHandlerException {
    if (Boolean.parseBoolean(envProvider.apply(ENV_USE_UDEV))) {
      LOG.info("Using libudev to retrieve syspath & device status");
      useUdev = true;
      udev.init();
      discoverer = new VEDeviceDiscoverer(udev);
    } else {
      scriptBasedInit(envProvider, scriptPaths);
    }
  }

  /**
   * 通过外部脚本方式初始化插件，查找设备发现脚本路径。
   * 按优先级依次从环境变量、HADOOP_COMMON_HOME、默认搜索路径查找脚本。
   * @param envProvider 环境变量获取函数
   * @param scriptPaths 脚本搜索路径数组
   * @throws ResourceHandlerException 未找到可用脚本时抛出异常
   */
  private void scriptBasedInit(Function<String, String> envProvider,
      String[] scriptPaths) throws ResourceHandlerException {
    String binaryName = DEFAULT_SCRIPT_NAME;

    String envScriptName = envProvider.apply(ENV_SCRIPT_NAME);
    if (envScriptName != null) {
      binaryName = envScriptName;
    }
    LOG.info("Use {} as script name.", binaryName);

    // 1. 优先从环境变量指定路径查找脚本
    boolean found = false;
    String envBinaryPath = envProvider.apply(ENV_SCRIPT_PATH);
    if (envBinaryPath != null) {
      this.binaryPath = getScriptFromEnvSetting(envBinaryPath);
      found = binaryPath != null;
    }

    // 2. 环境变量未找到，尝试从$HADOOP_COMMON_HOME查找
    if (!found) {
      // 仅当环境变量设置过但找不到时打印警告
      if (envBinaryPath != null) {
        LOG.warn("Script {} does not exist, falling back " +
            "to $HADOOP_COMMON_HOME/sbin/DevicePluginScript/", envBinaryPath);
      }

      this.binaryPath = getScriptFromHadoopCommon(envProvider, binaryName);
      found = binaryPath != null;
    }

    // 3. HADOOP_COMMON_HOME未找到，尝试默认搜索路径
    if (!found) {
      LOG.info("Script not found under" +
          " $HADOOP_COMMON_HOME/sbin/DevicePluginScript/," +
          " falling back to default search directories");

      this.binaryPath = getScriptFromSearchDirs(binaryName, scriptPaths);
      found = binaryPath != null;
    }

    // 所有路径都未找到脚本，抛出异常
    if (!found) {
      LOG.error("Script not found in "
          + Arrays.toString(scriptPaths));
      throw new ResourceHandlerException(
          "No binary found for " + NECVEPlugin.class.getName());
    }
  }

  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() {
    // 注册资源名为nec.com/ve，对应NEC VE设备资源
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("nec.com/ve").build();
  }

  @Override
  public Set<Device> getDevices() {
    Set<Device> devices = null;

    // 根据配置选择设备发现方式
    if (useUdev) {
      try {
        // 使用udev枚举/dev下的VE设备
        devices = discoverer.getDevicesFromPath("/dev");
      } catch (IOException e) {
        LOG.error("Error during scanning devices", e);
      }
    } else {
      // 使用外部脚本发现设备
      CommandExecutor executor =
          commandExecutorProvider.apply(new String[]{this.binaryPath});
      try {
        executor.execute();
        String output = executor.getOutput();
        // 解析脚本输出获取设备信息
        devices = parseOutput(output);
      } catch (IOException e) {
        LOG.error("Error during executing external binary", e);
      }
    }

    if (devices != null) {
      LOG.info("Found devices:");
      devices.forEach(dev -> LOG.info("{}", dev));
    }

    return devices;
  }

  @Override
  public DeviceRuntimeSpec onDevicesAllocated(Set<Device> set,
      YarnRuntimeType yarnRuntimeType) {
    // 不需要额外运行时配置，返回null即可
    return null;
  }

  /**
   * 解析外部Python设备发现脚本的输出，提取设备信息。
   * 输出格式示例：id=0, dev=/dev/ve0, state=ONLINE, busId=0000:65:00.0, major=243, minor=0
   *
   * @param output 脚本输出字符串
   * @return 解析得到的可用设备集合
   */
  private Set<Device> parseOutput(String output) {
    Set<Device> devices = new HashSet<>();

    LOG.info("Parsing output: {}", output);
    String[] lines = output.split("\n");
    outer:
    for (String line : lines) {
      Device.Builder builder = Device.Builder.newInstance();

      // 构建键值对到DeviceBuilder方法的映射
      Map<String, Consumer<String>> builderInvocations =
          getBuilderInvocationsMap(builder);

      String[] keyValues = line.trim().split(",");
      for (String keyValue : keyValues) {
        String[] tokens = keyValue.trim().split("=");
        if (tokens.length != 2) {
          LOG.error("Unknown format of script output! Skipping this line");
          continue outer;
        }

        final String key = tokens[0];
        final String value = tokens[1];

        Consumer<String> builderInvocation = builderInvocations.get(key);
        if (builderInvocation != null) {
          // 调用对应方法设置设备属性
          builderInvocation.accept(value);
        } else {
          // 忽略未知属性，打印警告
          LOG.warn("Unknown key {}, ignored", key);
        }
      }// for key value pairs
      Device device = builder.build();
      // 只添加健康状态的设备到可用集合
      if (device.isHealthy()) {
        devices.add(device);
      } else {
        LOG.warn("Skipping device {} because it's not healthy", device);
      }
    }

    return devices;
  }

  @Override
  public void onDevicesReleased(Set<Device> releasedDevices) {
    // 设备释放后无需额外处理，空实现
  }

  @Override
  public Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> env) {
    // 简单轮询分配策略：按顺序分配前count个可用设备
    // 未来可扩展考虑拓扑、利用率等因素优化分配
    Set<Device> allocated = new HashSet<>();
    int number = 0;
    for (Device d : availableDevices) {
      allocated.add(d);
      number++;
      if (number == count) {
        break;
      }
    }
    return allocated;
  }

  /**
   * 创建Shell命令执行器实例。
   * @param command 要执行的命令数组
   * @return 命令执行器实例
   */
  private CommandExecutor createCommandExecutor(String[] command) {
    return new Shell.ShellCommandExecutor(
        command);
  }

  /**
   * 从环境变量指定路径获取脚本，验证路径有效性和可执行权限。
   * @param envBinaryPath 环境变量指定的脚本路径
   * @return 验证通过返回路径，否则返回null
   */
  private String getScriptFromEnvSetting(String envBinaryPath) {
    LOG.info("Checking script path: {}", envBinaryPath);
    File f = new File(envBinaryPath);

    if (!f.exists()) {
      LOG.warn("Script {} does not exist", envBinaryPath);
      return null;
    }

    if (f.isDirectory()) {
      LOG.warn("Specified path {} is a directory", envBinaryPath);
      return null;
    }

    if (!FileUtil.canExecute(f)) {
      LOG.warn("Script {} is not executable", envBinaryPath);
      return null;
    }

    LOG.info("Found script: {}", envBinaryPath);

    return envBinaryPath;
  }

  /**
   * 从$HADOOP_COMMON_HOME目录查找设备发现脚本。
   * @param envProvider 环境变量获取函数
   * @param binaryName 脚本名称
   * @return 找到返回脚本路径，否则返回null
   */
  private String getScriptFromHadoopCommon(
      Function<String, String> envProvider, String binaryName) {
    String scriptPath = null;
    String hadoopCommon = envProvider.apply(HADOOP_COMMON_HOME);

    if (hadoopCommon != null) {
      String targetPath = hadoopCommon +
          "/sbin/DevicePluginScript/" + binaryName;
      LOG.info("Checking script {}: ", targetPath);
      if (new File(targetPath).exists()) {
        LOG.info("Found script: {}", targetPath);
        scriptPath = targetPath;
      }
    } else {
      LOG.info("$HADOOP_COMMON_HOME is not set");
    }

    return scriptPath;
  }

  /**
   * 从默认搜索路径数组查找设备发现脚本。
   * @param binaryName 脚本名称
   * @param scriptPaths 搜索路径数组
   * @return 找到返回脚本绝对路径，否则返回null
   */
  private String getScriptFromSearchDirs(String binaryName,
      String[] scriptPaths) {
    String scriptPath = null;

    for (String dir : scriptPaths) {
      File f = new File(dir, binaryName);
      if (f.exists()) {
        LOG.info("Found script: {}", dir);
        scriptPath = f.getAbsolutePath();
        break;
      }
    }

    return scriptPath;
  }

  /**
   * 创建设备属性键到DeviceBuilder setter方法的映射表。
   * 用于快速解析键值对输出。
   * @param builder Device构建器实例
   * @return 属性映射表
   */
  private Map<String, Consumer<String>> getBuilderInvocationsMap(
      Device.Builder builder) {
    Map<String, Consumer<String>> builderInvocations = new HashMap<>();
    builderInvocations.put("id", v -> builder.setId(Integer.parseInt(v)));
    builderInvocations.put("dev", v -> builder.setDevPath(v));
    builderInvocations.put("state", v -> {
      if (v.equals("ONLINE")) {
        builder.setHealthy(true);
      }
      builder.setStatus(v);
    });
    builderInvocations.put("busId", v -> builder.setBusID(v));
    builderInvocations.put("major",
        v -> builder.setMajorNumber(Integer.parseInt(v)));
    builderInvocations.put("minor",
        v -> builder.setMinorNumber(Integer.parseInt(v)));

    return builderInvocations;
  }

  /**
   * 设置命令执行器工厂，用于单元测试注入mock执行器。
   * @param provider 命令执行器工厂
   */
  @VisibleForTesting
  void setCommandExecutorProvider(
      Function<String[], CommandExecutor> provider) {
    this.commandExecutorProvider = provider;
  }

  /**
   * 设置设备发现器，用于单元测试注入mock发现器。
   * @param veDeviceDiscoverer 设备发现器实例
   */
  @VisibleForTesting
  void setVeDeviceDiscoverer(VEDeviceDiscoverer veDeviceDiscoverer) {
    this.discoverer = veDeviceDiscoverer;
  }

  /**
   * 获取设备发现脚本路径，用于单元测试。
   * @return 脚本路径
   */
  @VisibleForTesting
  String getBinaryPath() {
    return binaryPath;
  }
}