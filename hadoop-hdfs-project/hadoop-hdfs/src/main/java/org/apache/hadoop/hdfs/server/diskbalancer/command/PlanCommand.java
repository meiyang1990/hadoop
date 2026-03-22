// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.command;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Throwables;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.io.PrintStream;

/**
 * 磁盘均衡器生成均衡计划的命令实现类。
 * <p>
 * 计划命令读取集群拓扑信息，为指定数据节点生成磁盘均衡移动计划，最终将计划输出到指定路径供后续执行。
 */
public class PlanCommand extends Command {
  private double thresholdPercentage;
  private int bandwidth;
  private int maxError;

  /**
   * 构造PlanCommand对象，使用默认输出流。
   * @param conf Hadoop配置对象
   */
  public PlanCommand(Configuration conf) {
    this(conf, System.out);
  }

  /**
   * 构造PlanCommand对象，指定配置和输出流。
   * @param conf Hadoop配置对象
   * @param ps 输出打印流
   */
  public PlanCommand(Configuration conf, final PrintStream ps) {
    super(conf, ps);
    // 初始化默认参数
    this.thresholdPercentage = 1;
    this.bandwidth = 0;
    this.maxError = 0;
    // 注册命令支持的参数及说明
    addValidCommandParameters(DiskBalancerCLI.OUTFILE, "Output directory in " +
        "HDFS. The generated plan will be written to a file in this " +
        "directory.");
    addValidCommandParameters(DiskBalancerCLI.BANDWIDTH,
        "Maximum Bandwidth to be used while copying.");
    addValidCommandParameters(DiskBalancerCLI.THRESHOLD,
        "Percentage skew that we tolerate before diskbalancer starts working.");
    addValidCommandParameters(DiskBalancerCLI.MAXERROR,
        "Max errors to tolerate between 2 disks");
    addValidCommandParameters(DiskBalancerCLI.VERBOSE, "Run plan command in " +
        "verbose mode.");
    addValidCommandParameters(DiskBalancerCLI.PLAN, "Plan Command");
  }

  /**
   * 执行生成磁盘均衡计划的主逻辑，支持通过IP、主机名、数据节点UUID指定目标节点。
   * @param cmd 命令行参数对象
   * @throws Exception 执行过程中抛出的异常
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    TextStringBuilder result = new TextStringBuilder();
    String outputLine = "";
    LOG.debug("Processing Plan Command.");
    // 检查必须存在plan参数
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.PLAN));
    verifyCommandOptions(DiskBalancerCLI.PLAN, cmd);

    // 检查必须指定目标节点名称
    if (cmd.getOptionValue(DiskBalancerCLI.PLAN) == null) {
      throw new IllegalArgumentException("A node name is required to create a" +
          " plan.");
    }

    // 解析用户指定的带宽限制
    if (cmd.hasOption(DiskBalancerCLI.BANDWIDTH)) {
      this.bandwidth = Integer.parseInt(cmd.getOptionValue(DiskBalancerCLI
          .BANDWIDTH));
    }

    // 解析用户指定的最大错误容忍数
    if (cmd.hasOption(DiskBalancerCLI.MAXERROR)) {
      this.maxError = Integer.parseInt(cmd.getOptionValue(DiskBalancerCLI
          .MAXERROR));
    }

    // 读取集群拓扑和磁盘使用信息
    readClusterInfo(cmd);
    String output = null;
    if (cmd.hasOption(DiskBalancerCLI.OUTFILE)) {
      output = cmd.getOptionValue(DiskBalancerCLI.OUTFILE);
    }
    setOutputPath(output);

    // 根据参数获取目标数据节点信息
    DiskBalancerDataNode node =
        getNode(cmd.getOptionValue(DiskBalancerCLI.PLAN));
    if (node == null) {
      throw new IllegalArgumentException("Unable to find the specified node. " +
          cmd.getOptionValue(DiskBalancerCLI.PLAN));
    }

    // 将集群信息写入前置快照文件，用于后续对比
    try (FSDataOutputStream beforeStream = create(String.format(
        DiskBalancerCLI.BEFORE_TEMPLATE,
        cmd.getOptionValue(DiskBalancerCLI.PLAN)))) {
      beforeStream.write(getCluster().toJson()
          .getBytes(StandardCharsets.UTF_8));
    }

    // 获取均衡阈值百分比
    this.thresholdPercentage = getThresholdPercentage(cmd);

    LOG.debug("threshold Percentage is {}", this.thresholdPercentage);
    setNodesToProcess(node);
    populatePathNames(node);

    NodePlan plan = null;
    // 调用集群计算生成均衡计划
    List<NodePlan> plans = getCluster().computePlan(this.thresholdPercentage);
    // 将用户指定参数设置到计划步骤中
    setPlanParams(plans);

    if (plans.size() > 0) {
      plan = plans.get(0);
    }

    try {
      // 如果生成了有效计划，输出到计划文件
      if (plan != null && plan.getVolumeSetPlans().size() > 0) {
        outputLine = String.format("Writing plan to:");
        recordOutput(result, outputLine);

        final String planFileName = String.format(
            DiskBalancerCLI.PLAN_TEMPLATE,
            cmd.getOptionValue(DiskBalancerCLI.PLAN));
        final String planFileFullName =
            new Path(getOutputPath(), planFileName).toString();
        recordOutput(result, planFileFullName);

        // 将计划以JSON格式写入HDFS
        try (FSDataOutputStream planStream = create(planFileName)) {
          planStream.write(plan.toJson().getBytes(StandardCharsets.UTF_8));
        }
      } else {
        // 无需均衡，输出提示信息
        outputLine = String.format(
            "No plan generated. DiskBalancing not needed for node: %s"
                + " threshold used: %s",
            cmd.getOptionValue(DiskBalancerCLI.PLAN), this.thresholdPercentage);
        recordOutput(result, outputLine);
      }

      //  verbose模式下将计划详情打印到控制台
      if (cmd.hasOption(DiskBalancerCLI.VERBOSE) && plans.size() > 0) {
        printToScreen(plans);
      }
    } catch (Exception e) {
      final String errMsg =
          "Errors while recording the output of plan command.";
      LOG.error(errMsg, e);
      result.appendln(errMsg).appendln(Throwables.getStackTraceAsString(e));
    }

    // 输出最终执行结果
    getPrintStream().print(result.toString());
  }


  /**
   * 打印Plan命令的帮助信息到控制台。
   */
  @Override
  public void printHelp() {
    String header = "Creates a plan that describes how much data should be " +
        "moved between disks.\n\n";

    String footer = "\nPlan command creates a set of steps that represent a " +
        "planned data move. A plan file can be executed on a data node, which" +
        " will balance the data.";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -plan <hostname> [options]",
        header, DiskBalancerCLI.getPlanOptions(), footer);
  }

  /**
   * 获取磁盘均衡阈值百分比，优先使用命令行参数，参数非法时回退到配置默认值。
   * @param cmd 命令行参数对象
   * @return 有效阈值百分比
   */
  private double getThresholdPercentage(CommandLine cmd) {
    Double value = 0.0;
    if (cmd.hasOption(DiskBalancerCLI.THRESHOLD)) {
      value = Double.parseDouble(cmd.getOptionValue(DiskBalancerCLI.THRESHOLD));
    }

    // 阈值范围必须在(0,100]，非法则读取配置默认值
    if ((value <= 0.0) || (value > 100.0)) {
      value = getConf().getDouble(
          DFSConfigKeys.DFS_DISK_BALANCER_PLAN_THRESHOLD,
          DFSConfigKeys.DFS_DISK_BALANCER_PLAN_THRESHOLD_DEFAULT);
    }
    return value;
  }

  /**
   * 格式化打印计划详情到控制台，用于verbose模式展示。
   * @param plans 数据节点均衡计划列表
   */
  static private void printToScreen(List<NodePlan> plans) {
    System.out.println("\nPlan :\n");
    System.out.println(StringUtils.repeat("=", 80));

    // 打印表头
    System.out.println(
        StringUtils.center("Source Disk", 30) +
            StringUtils.center("Dest.Disk", 30) +
            StringUtils.center("Size", 10) +
            StringUtils.center("Type", 10));

    // 遍历打印每个移动步骤
    for (NodePlan plan : plans) {
      for (Step step : plan.getVolumeSetPlans()) {
        System.out.println(String.format("%s %s %s %s",
            StringUtils.center(step.getSourceVolume().getPath(), 30),
            StringUtils.center(step.getDestinationVolume().getPath(), 30),
            StringUtils.center(step.getSizeString(step.getBytesToMove()), 10),
            StringUtils.center(step.getDestinationVolume().getStorageType(),
                10)));
      }
    }

    System.out.println(StringUtils.repeat("=", 80));
  }

  /**
   * 将用户指定的带宽和最大错误容忍参数设置到所有计划步骤中。
   * @param plans 数据节点均衡计划列表
   */
  private void setPlanParams(List<NodePlan> plans) {
    for (NodePlan plan : plans) {
      for (Step step : plan.getVolumeSetPlans()) {
        if (this.bandwidth > 0) {
          LOG.debug("Setting bandwidth to {}", this.bandwidth);
          step.setBandwidth(this.bandwidth);
        }
        if (this.maxError > 0) {
          LOG.debug("Setting max error to {}", this.maxError);
          step.setMaxDiskErrors(this.maxError);
        }
      }
    }
  }
}