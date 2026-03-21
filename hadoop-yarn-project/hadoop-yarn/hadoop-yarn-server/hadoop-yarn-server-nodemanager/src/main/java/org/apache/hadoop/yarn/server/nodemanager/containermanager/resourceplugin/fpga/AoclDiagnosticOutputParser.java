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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFpgaOpenclPlugin.InnerShellExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 解析 Intel FPGA aocl diagnose 命令的输出，提取可用FPGA设备信息
 */
final class AoclDiagnosticOutputParser {
  private AoclDiagnosticOutputParser() {
    // no instances
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      AoclDiagnosticOutputParser.class);

  /**
   * One real sample output of Intel FPGA SDK 17.0's "aocl diagnose" is as below:
   * "
   * aocl diagnose: Running diagnose from /home/fpga/intelFPGA_pro/17.0/hld/board/nalla_pcie/linux64/libexec
   *
   * ------------------------- acl0 -------------------------
   * Vendor: Nallatech ltd
   *
   * Phys Dev Name  Status   Information
   *
   * aclnalla_pcie0Passed   nalla_pcie (aclnalla_pcie0)
   *                        PCIe dev_id = 2494, bus:slot.func = 02:00.00, Gen3 x8
   *                        FPGA temperature = 54.4 degrees C.
   *                        Total Card Power Usage = 31.7 Watts.
   *                        Device Power Usage = 0.0 Watts.
   *
   * DIAGNOSTIC_PASSED
   * ---------------------------------------------------------
   * "
   *
   * While per Intel's guide, the output(should be outdated or prior SDK version's) is as below:
   *
   * "
   * aocl diagnose: Running diagnostic from ALTERAOCLSDKROOT/board/&lt;board_name&gt;/
   * &lt;platform&gt;/libexec
   * Verified that the kernel mode driver is installed on the host machine.
   * Using board package from vendor: &lt;board_vendor_name&gt;
   * Querying information for all supported devices that are installed on the host
   * machine ...
   *
   * device_name Status Information
   *
   * acl0 Passed &lt;descriptive_board_name&gt;
   *             PCIe dev_id = &lt;device_ID&gt;, bus:slot.func = 02:00.00,
   *               at Gen 2 with 8 lanes.
   *             FPGA temperature=43.0 degrees C.
   * acl1 Passed &lt;descriptive_board_name&gt;
   *             PCIe dev_id = &lt;device_ID&gt;, bus:slot.func = 03:00.00,
   *               at Gen 2 with 8 lanes.
   *             FPGA temperature = 35.0 degrees C.
   *
   * Found 2 active device(s) installed on the host machine, to perform a full
   * diagnostic on a specific device, please run aocl diagnose &lt;device_name&gt;
   *
   * DIAGNOSTIC_PASSED
   * "
   * But this method only support the first output
   *
   * 解析aocl diagnose命令输出，提取可用FPGA设备信息
   * @param output aocl diagnose命令的标准输出
   * @param shellExecutor shell执行器，用于获取设备主从设备号
   * @param fpgaType FPGA设备类型
   * @return 解析得到的可用FPGA设备列表，解析失败返回空列表
   * */
  public static List<FpgaDevice> parseDiagnosticOutput(
      String output, InnerShellExecutor shellExecutor, String fpgaType) {
    // 检查诊断是否成功完成
    if (output.contains("DIAGNOSTIC_PASSED")) {
      List<FpgaDevice> devices = new ArrayList<>();
      // 匹配FPGA设备别名开头（acl0~acl31）
      Matcher headerStartMatcher = Pattern.compile("acl[0-31]")
          .matcher(output);
      // 匹配诊断结束标记，不区分大小写
      Matcher headerEndMatcher = Pattern.compile("(?i)DIAGNOSTIC_PASSED")
          .matcher(output);
      int sectionStartIndex;
      int sectionEndIndex;
      String aliasName;

      // 遍历所有匹配到的FPGA设备
      while (headerStartMatcher.find()) {
        // 获取设备块起始位置
        sectionStartIndex = headerStartMatcher.end();
        String section = null;
        // 获取当前设备别名
        aliasName = headerStartMatcher.group();
        // 查找当前设备块结束位置
        while (headerEndMatcher.find(sectionStartIndex)) {
          sectionEndIndex = headerEndMatcher.start();
          // 截取当前设备的文本块
          section = output.substring(sectionStartIndex, sectionEndIndex);
          break;
        }

        // 未找到合法设备块，返回空列表
        if (section == null) {
          LOG.warn("Unsupported diagnose output");
          LOG.warn("aocl output is: " + output);
          return Collections.emptyList();
        }

        // 定义需要提取的字段正则表达式：设备名、总线信息、温度、功耗
        String[] fieldRegexes = new String[]{"\\(.*\\)\n",
            "(?i)bus:slot.func\\s=\\s.*,",
            "(?i)FPGA temperature\\s=\\s.*",
            "(?i)Total\\sCard\\sPower\\sUsage\\s=\\s.*"};
        String[] fields = new String[4];
        String tempFieldValue;

        // 逐个提取字段
        for (int i = 0; i < fieldRegexes.length; i++) {
          Matcher fieldMatcher = Pattern.compile(fieldRegexes[i])
              .matcher(section);
          if (!fieldMatcher.find()) {
            LOG.warn("Couldn't find " + fieldRegexes[i] + " pattern");
            fields[i] = "";
            continue;
          }
          // 获取匹配到的字段值并去除首尾空白
          tempFieldValue = fieldMatcher.group().trim();
          if (i == 0) {
            // 设备名字段特殊处理：去除括号
            fields[i] = tempFieldValue.substring(1,
                tempFieldValue.length() - 1);
          } else {
            // 其他字段分割出=号后的值，并去除末尾逗号
            String ss = tempFieldValue.split("=")[1].trim();
            fields[i] = ss.substring(0, ss.length() - 1);
          }
        }

        // 通过shell执行器获取设备主从设备号
        String majorMinorNumber = shellExecutor
            .getMajorAndMinorNumber(fields[0]);
        if (null != majorMinorNumber) {
          // 分割主设备号和从设备号
          String[] mmn = majorMinorNumber.split(":");

          // 添加解析完成的FPGA设备到列表
          devices.add(new FpgaDevice(fpgaType,
              Integer.parseInt(mmn[0]),
              Integer.parseInt(mmn[1]),
              aliasName));
        } else {
          LOG.warn("Failed to retrieve major/minor number for device");
        }
      }

      // 返回解析得到的所有FPGA设备
      return devices;
    } else {
      // 诊断执行失败，记录日志返回空列表
      LOG.warn("The diagnostic has failed");
      LOG.warn("Output of aocl is: " + output);
      return Collections.emptyList();
    }
  }
}