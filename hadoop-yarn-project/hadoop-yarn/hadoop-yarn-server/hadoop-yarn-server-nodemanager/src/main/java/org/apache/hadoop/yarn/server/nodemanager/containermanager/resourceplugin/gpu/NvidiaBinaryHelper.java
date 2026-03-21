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

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformationParser;

/**
 * 调用Nvidia官方nvidia-smi命令获取GPU设备信息，并解析返回结构化数据
 * 用于YARN NodeManager节点GPU资源发现与信息采集
 *
 */
public class NvidiaBinaryHelper {

  /**
   * 执行nvidia-smi命令获取并解析当前节点GPU设备信息
   * @param pathOfGpuBinary nvidia-smi可执行文件路径
   * @param discoveryTimeoutMs 命令执行超时时间
   * @return 解析完成的GPU设备信息对象
   * @throws IOException 无法读取命令输出时抛出
   * @throws YarnException 可执行文件路径为空或输出解析失败时抛出
   */
  synchronized GpuDeviceInformation getGpuDeviceInformation(
      String pathOfGpuBinary, long discoveryTimeoutMs)
      throws IOException, YarnException {
    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();

    if (pathOfGpuBinary == null) {
      throw new YarnException(
          "Failed to find GPU discovery executable, please double check "
              + YarnConfiguration.NM_GPU_PATH_TO_EXEC + " setting.");
    }

    // 执行nvidia-smi命令，指定-x参数输出XML格式查询结果
    String output = Shell.execCommand(new HashMap<>(),
        new String[]{pathOfGpuBinary, "-x", "-q"}, discoveryTimeoutMs);
    // 解析XML输出为结构化GPU设备信息对象并返回
    return parser.parseXml(output);
  }
}