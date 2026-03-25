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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * YARN NodeManager FPGA资源插件的抽象接口，供不同FPGA厂商实现自定义逻辑
 * 被{@link FpgaDiscoverer}和{@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceHandlerImpl}
 * 用于FPGA设备发现、FPGA比特流(IP)下载、IP配置流程
 * */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AbstractFpgaVendorPlugin {

  /**
   * 初始化厂商插件，检查工具链和运行环境是否满足要求
   * @param conf Hadoop配置对象
   * @return 初始化成功返回true，失败返回false
   * */
  boolean initPlugin(Configuration conf);

  /**
   * 使用厂商工具链对FPGA设备进行健康诊断，无需解析设备信息
   *
   * @param timeout 诊断超时时间，单位毫秒
   * @return 诊断成功返回true，失败返回false
   * */
  boolean diagnose(int timeout);

  /**
   * 发现当前节点上该厂商的所有FPGA设备，受执行时间限制
   * @param timeout 发现操作的最大允许超时时间
   * @return 发现到的FPGA设备列表，结果会被添加到FPGAResourceAllocator供后续调度使用
   * */
  List<FpgaDevice> discover(int timeout);

  /**
   * 获取当前厂商FPGA设备类型标识，所有厂商插件共享同一个FpgaResourceAllocator，需要通过类型区分不同厂商的设备
   *
   * @return FPGA设备类型字符串标识
   * */
  String getFpgaType();

  /**
   * 获取应用所需的FPGA比特流(IP)文件路径，若文件未下载则下载到目标目录，需检查已下载缓存
   * @param id IP文件标识符，来自应用请求，如 matrix_multi_v1
   * @param dstDir 下载目标目录
   * @param localizedResources 容器本地化资源，可从中查找已本地化的IP文件，key为本地化文件路径，value为软链接名称列表
   * @return IP文件的绝对路径字符串
   * */
  String retrieveIPfilePath(String id, String dstDir,
      Map<Path, List<String>> localizedResources);

  /**
   * 将指定IP文件配置到指定FPGA设备上完成烧录
   * @param ipPath IP文件的绝对路径
   * @param device 目标FPGA设备对象
   * @return 配置成功返回true，失败返回false
   * */
  boolean configureIP(String ipPath, FpgaDevice device);
}