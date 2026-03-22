// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.util.Preconditions;


/**
 * 文件说明：磁盘均衡器report命令实现类，负责查询并输出DataNode磁盘分布信息
 * 功能说明：查询指定DataNode的磁盘卷信息，或查询按数据密度排序后收益最高的Top N DataNode，
 * 帮助用户识别哪些节点运行磁盘均衡器可以获得最大收益。
 * 实现逻辑：读取集群拓扑信息，按节点数据密度排序后输出结果，支持指定节点查询和Top节点查询两种模式。
 */
public class ReportCommand extends Command {

  /**
   * 构造ReportCommand，使用默认标准输出作为输出流
   * @param conf Hadoop配置对象
   */
  public ReportCommand(Configuration conf) {
    this(conf, System.out);
  }

  /**
   * 构造ReportCommand，使用指定输出流输出结果
   * @param conf Hadoop配置对象
   * @param ps 输出流，用于打印报告结果
   */
  public ReportCommand(Configuration conf, final PrintStream ps) {
    super(conf, ps);

    addValidCommandParameters(DiskBalancerCLI.REPORT,
        "Report volume information of nodes.");

    String desc = String.format(
        "Top number of nodes to be processed. Default: %d", getDefaultTop());
    addValidCommandParameters(DiskBalancerCLI.TOP, desc);

    desc = String.format("Print out volume information for DataNode(s).");
    addValidCommandParameters(DiskBalancerCLI.NODE, desc);
  }

  /**
   * 执行report命令，解析命令行参数，生成并输出磁盘分布报告
   * @param cmd 命令行参数对象
   * @throws Exception 执行过程中可能抛出异常
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    TextStringBuilder result = new TextStringBuilder();
    String outputLine = "Processing report command";
    recordOutput(result, outputLine);

    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.REPORT));
    verifyCommandOptions(DiskBalancerCLI.REPORT, cmd);
    readClusterInfo(cmd);

    // 节点信息输出格式（带序号）
    final String nodeFormat =
        "%d/%d %s[%s:%d] - <%s>: %d volumes with node data density %.2f.";
    // 节点信息输出格式（不带序号）
    final String nodeFormatWithoutSequence =
        "%s[%s:%d] - <%s>: %d volumes with node data density %.2f.";
    // 卷信息输出格式
    final String volumeFormat =
        "[%s: volume-%s] - %.2f used: %d/%d, %.2f free: %d/%d, "
        + "isFailed: %s, isReadOnly: %s, isSkip: %s, isTransient: %s.";

    if (cmd.hasOption(DiskBalancerCLI.NODE)) {
      /*
       * Reporting volume information for specific DataNode(s)
       */
      handleNodeReport(cmd, result, nodeFormatWithoutSequence, volumeFormat);

    } else { // handle TOP
      /*
       * Reporting volume information for top X DataNode(s)
       */
      handleTopReport(cmd, result, nodeFormat);
    }

    getPrintStream().println(result.toString());
  }

  /**
   * 处理Top N模式，按节点数据密度降序排序，输出Top N最需要均衡的节点信息
   * @param cmd 命令行参数
   * @param result 结果字符串构建器
   * @param nodeFormat 节点输出格式字符串
   * @throws IllegalArgumentException 参数解析异常
   */
  private void handleTopReport(final CommandLine cmd, final TextStringBuilder result,
      final String nodeFormat) throws IllegalArgumentException {
    // 按数据密度降序排序节点
    Collections.sort(getCluster().getNodes(), Collections.reverseOrder());

    /* extract value that identifies top X DataNode(s) */
    // 解析并设置需要展示的Top节点数量
    setTopNodes(parseTopNodes(cmd, result));

    /*
     * Reporting volume information of top X DataNode(s) in summary
     */
    final String outputLine = String.format(
        "Reporting top %d DataNode(s) benefiting from running DiskBalancer.",
        getTopNodes());
    recordOutput(result, outputLine);

    // 遍历输出Top N节点，输出摘要信息
    ListIterator<DiskBalancerDataNode> li = getCluster().getNodes()
        .listIterator();

    for (int i = 0; i < getTopNodes() && li.hasNext(); i++) {
      DiskBalancerDataNode dbdn = li.next();
      result.appendln(String.format(nodeFormat,
          i+1,
          getTopNodes(),
          dbdn.getDataNodeName(),
          dbdn.getDataNodeIP(),
          dbdn.getDataNodePort(),
          dbdn.getDataNodeUUID(),
          dbdn.getVolumeCount(),
          dbdn.getNodeDataDensity()));
    }
  }

  /**
   * 处理指定节点查询模式，输出指定DataNode的所有磁盘卷详细信息
   * @param cmd 命令行参数
   * @param result 结果字符串构建器
   * @param nodeFormat 节点信息输出格式
   * @param volumeFormat 卷信息输出格式
   * @throws Exception 节点解析或读取信息异常
   */
  private void handleNodeReport(final CommandLine cmd, TextStringBuilder result,
      final String nodeFormat, final String volumeFormat) throws Exception {
    String outputLine = "";
    /*
     * get value that identifies DataNode(s) from command line, it could be
     * UUID, IP address or host name.
     */
    // 获取命令行指定的节点标识，可以是UUID、IP或主机名
    final String nodeVal = cmd.getOptionValue(DiskBalancerCLI.NODE);

    if (StringUtils.isBlank(nodeVal)) {
      outputLine = "The value for '-node' is neither specified or empty.";
      recordOutput(result, outputLine);
    } else {
      /*
       * Reporting volume information for specific DataNode(s)
       */
      outputLine = String.format(
          "Reporting volume information for DataNode(s). "
          + "These DataNode(s) are parsed from '%s'.", nodeVal);

      recordOutput(result, outputLine);

      List<DiskBalancerDataNode> dbdns;
      try {
        // 按指定标识解析获取目标DataNode列表
        dbdns = getNodes(nodeVal);
      } catch (DiskBalancerException e) {
        // If there are some invalid nodes that contained in nodeVal,
        // the exception will be threw.
        recordOutput(result, e.getMessage());
        return;
      }

      // 遍历每个节点，输出详细卷信息
      if (!dbdns.isEmpty()) {
        for (DiskBalancerDataNode node : dbdns) {
          recordNodeReport(result, node, nodeFormat, volumeFormat);
          result.append(System.lineSeparator());
        }
      }
    }
  }

  /**
   * 将单个DataNode及其所有卷的详细信息写入结果缓冲区
   * @param result 结果字符串构建器
   * @param dbdn 目标DataNode对象
   * @param nodeFormat 节点信息输出格式
   * @param volumeFormat 卷信息输出格式
   * @throws Exception 填充路径信息异常
   */
  private void recordNodeReport(TextStringBuilder result, DiskBalancerDataNode dbdn,
      final String nodeFormat, final String volumeFormat) throws Exception {
    final String trueStr = "True";
    final String falseStr = "False";

    // 填充卷存储路径信息
    populatePathNames(dbdn);
    // 输出节点基础信息
    result.appendln(String.format(nodeFormat,
        dbdn.getDataNodeName(),
        dbdn.getDataNodeIP(),
        dbdn.getDataNodePort(),
        dbdn.getDataNodeUUID(),
        dbdn.getVolumeCount(),
        dbdn.getNodeDataDensity()));

    // 收集所有卷信息，排序后输出
    List<String> volumeList = Lists.newArrayList();
    for (DiskBalancerVolumeSet vset : dbdn.getVolumeSets().values()) {
      for (DiskBalancerVolume vol : vset.getVolumes()) {
        volumeList.add(String.format(volumeFormat,
            vol.getStorageType(),
            vol.getPath(),
            vol.getUsedRatio(),
            vol.getUsed(),
            vol.getCapacity(),
            vol.getFreeRatio(),
            vol.getFreeSpace(),
            vol.getCapacity(),
            vol.isFailed() ? trueStr : falseStr,
            vol.isReadOnly() ? trueStr: falseStr,
            vol.isSkip() ? trueStr : falseStr,
            vol.isTransient() ? trueStr : falseStr));
      }
    }

    Collections.sort(volumeList);
    result.appendln(
        StringUtils.join(volumeList.toArray(), System.lineSeparator()));
  }

  /**
   * 打印report命令帮助信息，展示使用方式和参数说明
   */
  @Override
  public void printHelp() {
    String header = "Report command reports the volume information of given" +
        " datanode(s), or prints out the list of nodes that will benefit " +
        "from running disk balancer. Top defaults to " + getDefaultTop();
    String footer = ". E.g.:\n"
        + "hdfs diskbalancer -report\n"
        + "hdfs diskbalancer -report -top 5\n"
        + "hdfs diskbalancer -report "
        + "-node <file://> | [<DataNodeID|IP|Hostname>,...]";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -fs http://namenode.uri " +
        "-report [options]",
        header, DiskBalancerCLI.getReportOptions(), footer);
  }
}