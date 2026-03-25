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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Intel OpenCL架构FPGA资源插件，集成Intel官方工具链实现FPGA设备管理与IP配置
 * 核心设计要点：
 * 1. 使用Intel官方aocl工具链完成设备发现、FPGA比特流(IP)烧录，容器启动前提前烧录加速应用启动
 * 2. 通过维护设备与已烧录IP的映射关系，避免重复烧录提升效率
 * 3. 假设IP比特流文件已经通过YARN本地化分发到容器目录，无需额外下载
 */
public class IntelFpgaOpenclPlugin implements AbstractFpgaVendorPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntelFpgaOpenclPlugin.class);

  private boolean initialized = false;
  private InnerShellExecutor shell;

  private static final String DEFAULT_BINARY_NAME = "aocl";

  private static final String ALTERAOCLSDKROOT_NAME = "ALTERAOCLSDKROOT";

  private Function<String, String> envProvider = System::getenv;

  private String pathToExecutable = null;

  @VisibleForTesting
  void setInnerShellExecutor(InnerShellExecutor shellExecutor) {
    this.shell = shellExecutor;
  }

  @VisibleForTesting
  String getPathToExecutable() {
    return pathToExecutable;
  }

  @VisibleForTesting
  void setEnvProvider(Function<String, String> envProvider) {
    this.envProvider = envProvider;
  }

  public IntelFpgaOpenclPlugin() {
    this.shell = new InnerShellExecutor();
  }

  /**
   * 从系统环境变量获取aocl工具默认路径。
   * @return 默认路径
   */
  public String getDefaultPathToExecutable() {
    return envProvider.apply(ALTERAOCLSDKROOT_NAME);
  }

  /**
   * 初始化Intel FPGA插件，检测工具链可用性。
   * @param config YARN配置
   * @return 初始化是否成功
   */
  @Override
  public boolean initPlugin(Configuration config) {
    if (initialized) {
      return true;
    }

    // 从配置读取可执行文件名称
    String pluginDefaultBinaryName = DEFAULT_BINARY_NAME;
    String executable = config.get(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        pluginDefaultBinaryName);

    // 验证配置路径下文件是否存在
    File binaryPath = new File(executable);
    if (!binaryPath.exists()) {
      // 配置路径不存在，回退尝试默认路径
      LOG.warn("Failed to find FPGA discoverer executable configured in " +
          YarnConfiguration.NM_FPGA_PATH_TO_EXEC +
          ", please check! Try default path");
      executable = pluginDefaultBinaryName;
      // 从环境变量获取SDK路径尝试查找
      String pluginDefaultPreferredPath = getDefaultPathToExecutable();
      if (null == pluginDefaultPreferredPath) {
        LOG.warn("Failed to find FPGA discoverer executable from system "
            + " environment " + ALTERAOCLSDKROOT_NAME +
            ", please check your environment!");
      } else {
        binaryPath = new File(pluginDefaultPreferredPath + "/bin",
            pluginDefaultBinaryName);
        if (binaryPath.exists()) {
          // 环境变量路径下找到工具，使用该路径
          executable = binaryPath.getAbsolutePath();
          LOG.info("Succeed in finding FPGA discoverer executable: " +
              executable);
        } else {
          // 仍未找到，使用默认名称依赖PATH环境查找
          executable = pluginDefaultBinaryName;
          LOG.warn("Failed to find FPGA discoverer executable in " +
              pluginDefaultPreferredPath +
              ", file doesn't exists! Use default binary" + executable);
        }
      }
    }

    pathToExecutable = executable;

    // 执行诊断检测工具链是否正常可用
    if (!diagnose(10*1000)) {
      LOG.warn("Intel FPGA for OpenCL diagnose failed!");
      initialized = false;
    } else {
      initialized = true;
    }
    return initialized;
  }

  @Override
  public List<FpgaDevice> discover(int timeout) {
    List<FpgaDevice> list = new LinkedList<>();
    String output;
    // 执行aocl diagnose获取设备信息
    output = getDiagnoseInfo(timeout);
    if (null == output) {
      return list;
    }

    // 解析诊断输出，提取FPGA设备信息
    list = AoclDiagnosticOutputParser.parseDiagnosticOutput(output,
        shell, getFpgaType());

    return list;
  }

  /**
   *  内部工具类，负责执行shell命令获取FPGA设备信息、运行诊断。
   */
  public static class InnerShellExecutor {

    /**
     * 获取指定设备文件的主设备号/次设备号。
     * @param devName 设备名称
     * @return 主:次 格式字符串
     */
    public String getMajorAndMinorNumber(String devName) {
      String output = null;
      // 执行stat命令获取设备号十六进制值
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{"stat", "-c", "%t:%T", "/dev/" + devName});
      try {
        LOG.debug("Get FPGA major-minor numbers from /dev/{}", devName);
        shexec.execute();
        String[] strs = shexec.getOutput().trim().split(":");
        LOG.debug("stat output:{}", shexec.getOutput());
        // 将十六进制转换为十进制后拼接输出
        output = Integer.parseInt(strs[0], 16) + ":" +
            Integer.parseInt(strs[1], 16);
      } catch (IOException e) {
        LOG.warn("Failed to get major-minor number from reading /dev/" +
            devName);
        LOG.warn("Command output:" + shexec.getOutput() + ", exit code: " +
            shexec.getExitCode(), e);
      }
      return output;
    }

    /**
     * 执行aocl diagnose命令获取设备诊断输出。
     * @param binary aocl工具路径
     * @param timeout 命令执行超时时间
     * @return 诊断命令输出
     */
    public String runDiagnose(String binary, int timeout) {
      String output = null;
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{binary, "diagnose"}, null, null, timeout);
      try {
        shexec.execute();
      } catch (IOException e) {
        // aocl diagnose即使成功也会返回退出码1，因此忽略异常只保留输出
        String msg =
            "Failed to execute " + binary + " diagnose, exception message:" + e
                .getMessage() +", output:" + output + ", continue ...";
        LOG.warn(msg);
        LOG.debug("{}", shexec.getOutput());
      }
      return shexec.getOutput();
    }
  }

  public String getDiagnoseInfo(int timeout) {
    return this.shell.runDiagnose(this.pathToExecutable,timeout);
  }

  @Override
  public boolean diagnose(int timeout) {
    String output = getDiagnoseInfo(timeout);
    // 检查输出是否包含诊断通过标记
    if (null != output && output.contains("DIAGNOSTIC_PASSED")) {
      return true;
    }
    return false;
  }

  /**
   * 返回当前插件支持的FPGA类型标识。
   * @return 类型标识字符串
   * */
  @Override
  public String getFpgaType() {
    return "IntelOpenCL";
  }

  @Override
  public String retrieveIPfilePath(String id, String dstDir,
      Map<Path, List<String>> localizedResources) {
    // 假设.aocx格式IP文件已通过YARN本地化分发到本地目录，直接查找即可
    String ipFilePath = null;

    LOG.info("Got environment: " + id +
        ", search IP file in localized resources");
    if (null == id || id.isEmpty()) {
      LOG.warn("IP_ID environment is empty, skip downloading");
      return null;
    }

    if (localizedResources != null) {
      // 在已本地化资源中查找文件名匹配的aocx文件
      Optional<Path> aocxPath = localizedResources
          .keySet()
          .stream()
          .filter(path -> matchesIpid(path, id))
          .findFirst();

      if (aocxPath.isPresent()) {
        ipFilePath = aocxPath.get().toString();
        LOG.info("Found: {}", ipFilePath);
      } else {
        LOG.warn("Requested IP file not found");
      }
    } else {
      LOG.warn("Localized resource is null!");
    }

    return ipFilePath;
  }

  private boolean matchesIpid(Path p, String id) {
    // 匹配文件名：小写id + .aocx后缀，不区分大小写
    return p.getName().toLowerCase().equals(id.toLowerCase() + ".aocx");
  }

  /**
   * 将指定IP比特流烧录到目标FPGA设备。
   * 即使离线烧录失败也不影响容器启动，因为应用本身会在运行时重新编程
   * 提前离线烧录目的是加速应用启动过程
   * @param ipPath aocx IP文件绝对路径
   * @param device 目标FPGA设备对象
   * @return false 烧录失败，true 烧录成功
   * */
  @Override
  public boolean configureIP(String ipPath, FpgaDevice device) {
    // 提前执行离线烧录，加速后续应用启动流程
    // 需要获取设备别名构造aocl program命令: aocl program <acl设备名> <ip文件路径>
    Shell.ShellCommandExecutor shexec;
    String aclName;
    aclName = device.getAliasDevName();
    shexec = new Shell.ShellCommandExecutor(
        new String[]{this.pathToExecutable, "program", aclName, ipPath});
    try {
      shexec.execute();
      if (0 == shexec.getExitCode()) {
        LOG.debug("{}", shexec.getOutput());
        LOG.info("Intel aocl program " + ipPath + " to " +
            aclName + " successfully");
      } else {
        // 烧录命令执行失败
        LOG.error("Device programming failed, aocl output is:");
        LOG.error(shexec.getOutput());
        return false;
      }
    } catch (IOException e) {
      // 命令执行抛出IO异常
      LOG.error("Intel aocl program " + ipPath + " to " +
          aclName + " failed!", e);
      LOG.error("Aocl output: " + shexec.getOutput());
      return false;
    }
    return true;
  }
}