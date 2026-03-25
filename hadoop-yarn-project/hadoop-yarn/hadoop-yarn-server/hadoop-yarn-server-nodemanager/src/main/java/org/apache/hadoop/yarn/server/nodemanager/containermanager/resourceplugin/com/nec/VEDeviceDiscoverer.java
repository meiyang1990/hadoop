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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * NEC Vector Engine设备发现器，负责在当前NodeManager节点上探测并枚举VE设备
 */
class VEDeviceDiscoverer {
  private static final String STATE_TERMINATING = "TERMINATING";
  private static final String STATE_INITIALIZING = "INITIALIZING";
  private static final String STATE_OFFLINE = "OFFLINE";
  private static final String STATE_ONLINE = "ONLINE";
  private static final Logger LOG =
      LoggerFactory.getLogger(VEDeviceDiscoverer.class);

  // 按索引对应设备状态，索引和os_state文件中的数值对应
  private static final String[] DEVICE_STATE = {STATE_ONLINE, STATE_OFFLINE,
      STATE_INITIALIZING, STATE_TERMINATING};

  // Udev工具类，用于查询设备sysfs路径
  private UdevUtil udev;
  // 命令执行器工厂，支持注入用于测试
  private Function<String[], CommandExecutor>
      commandExecutorProvider = this::createCommandExecutor;

  /**
   * 构造VE设备发现器
   * @param udevUtil Udev工具实例
   */
  VEDeviceDiscoverer(UdevUtil udevUtil) {
    udev = udevUtil;
  }

  /**
   * 从指定路径扫描发现所有VE设备
   * @param path VE设备文件所在目录（通常是/dev）
   * @return 发现的VE设备集合
   * @throws IOException 扫描目录或读取设备信息失败时抛出
   */
  public Set<Device> getDevicesFromPath(String path) throws IOException {
    MutableInt counter = new MutableInt(0);
    try (Stream<Path> stream = Files.walk(Paths.get(path), 1)) {
      // 过滤出名称以veslot开头的设备文件，转换为Device对象并收集
      return stream.filter(p -> p.toFile().getName().startsWith("veslot"))
            .map(p -> toDevice(p, counter))
            .collect(Collectors.toSet());
    }
  }

  /**
   * 将VE设备文件转换为YARN Device对象
   * @param p 设备文件路径
   * @param counter 设备ID计数器
   * @return 构造完成的YARN Device对象
   */
  private Device toDevice(Path p, MutableInt counter) {
    // 执行stat命令获取设备主从设备号和设备类型
    CommandExecutor executor =
        commandExecutorProvider.apply(
            new String[]{"stat", "-L", "-c", "%t:%T:%F", p.toString()});

    try {
      LOG.info("Checking device file: {}", p);
      executor.execute();
      String statOutput = executor.getOutput();
      String[] stat = statOutput.trim().split(":");

      // 解析十六进制格式的主设备号
      int major = Integer.parseInt(stat[0], 16);
      // 解析十六进制格式的从设备号
      int minor = Integer.parseInt(stat[1], 16);
      // 判断设备类型（字符/块设备）
      char devType = getDevType(p, stat[2]);
      // 合成Linux dev_t设备编号
      int deviceNumber = makeDev(major, minor);
      LOG.info("Device: major: {}, minor: {}, devNo: {}, type: {}",
          major, minor, deviceNumber, devType);
      // 通过udev获取设备在sysfs中的路径
      String sysPath = udev.getSysPath(deviceNumber, devType);
      LOG.info("Device syspath: {}", sysPath);
      // 读取设备当前运行状态
      String deviceState = getDeviceState(sysPath);

      // 构造YARN Device对象
      Device.Builder builder = Device.Builder.newInstance();
      builder.setId(counter.getAndIncrement())
        .setMajorNumber(major)
        .setMinorNumber(minor)
        .setHealthy(STATE_ONLINE.equalsIgnoreCase(deviceState))
        .setStatus(deviceState)
        .setDevPath(p.toAbsolutePath().toString());

      return builder.build();
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot execute stat command", e);
    }
  }

  /**
   * 合成Linux内核格式的dev_t设备编号
   * @param major 主设备号
   * @param minor 从设备号
   * @return 合成后的dev_t编号
   */
  private int makeDev(int major, int minor) {
    return major * 256 + minor;
  }

  /**
   * 从stat输出判断设备类型
   * @param p 设备文件路径
   * @param fromStat stat命令输出的文件类型描述
   * @return 设备类型标识 'c'字符设备/'b'块设备
   */
  private char getDevType(Path p, String fromStat) {
    if (fromStat.contains("character")) {
      return 'c';
    } else if (fromStat.contains("block")) {
      return 'b';
    } else {
      throw new IllegalArgumentException(
          "File is neither a char nor block device: " + p);
    }
  }

  /**
   * 从sysfs读取VE设备的运行状态
   * @param sysPath 设备sysfs路径
   * @return 设备状态字符串
   * @throws IOException 读取os_state文件失败时抛出
   */
  private String getDeviceState(String sysPath) throws IOException {
    Path statePath = Paths.get(sysPath, "os_state");

    try (FileInputStream fis =
        new FileInputStream(statePath.toString())) {
      // 读取状态字节值
      byte state = (byte) fis.read();

      if (state < 0 || DEVICE_STATE.length <= state) {
        // 超出已知状态范围，返回未知状态
        return String.format("Unknown (%d)", state);
      } else {
        // 返回对应状态名称
        return DEVICE_STATE[state];
      }
    }
  }

  /**
   * 创建默认Shell命令执行器
   * @param command 要执行的命令参数数组
   * @return 命令执行器实例
   */
  private CommandExecutor createCommandExecutor(String[] command) {
    return new Shell.ShellCommandExecutor(
        command);
  }

  /**
   * 设置命令执行器工厂，用于单元测试注入mock执行器
   * @param provider 命令执行器工厂
   */
  @VisibleForTesting
  void setCommandExecutorProvider(
      Function<String[], CommandExecutor> provider) {
    this.commandExecutorProvider = provider;
  }
}