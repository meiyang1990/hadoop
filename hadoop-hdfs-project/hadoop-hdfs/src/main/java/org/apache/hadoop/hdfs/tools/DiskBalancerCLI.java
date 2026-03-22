// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.tools;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.diskbalancer.command.CancelCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.Command;
import org.apache.hadoop.hdfs.server.diskbalancer.command.ExecuteCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.HelpCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.PlanCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.QueryCommand;
import org.apache.hadoop.hdfs.server.diskbalancer.command.ReportCommand;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Arrays;

/**
 * HDFS磁盘均衡器命令行入口，负责将数据均匀分布到DataNode同类型存储磁盘之间。
 * 该工具可以在DataNode正常服务时运行，先计算出数据移动计划，再由DataNode执行移动操作。
 * 主要功能包括生成均衡计划、执行计划、查询执行状态、取消正在运行的均衡等。
 */
public class DiskBalancerCLI extends Configured implements Tool {
  /**
   * 生成磁盘均衡计划命令。
   */
  public static final String PLAN = "plan";
  /**
   * 输出文件路径，计划、报告等命令可选参数。
   * 默认会输出到集群当前目录的 /system/reports/diskbalancer 下。
   */
  public static final String OUTFILE = "out";
  /**
   * 帮助命令。
   */
  public static final String HELP = "help";
  /**
   * 可容忍的数据不均衡阈值百分比。
   * 表示允许实际存储量与理想值的偏差百分比，超过该阈值才会进行均衡。
   */
  public static final String THRESHOLD = "thresholdPercentage";
  /**
   * 指定每秒最大可使用的磁盘带宽。
   */
  public static final String BANDWIDTH = "bandwidth";
  /**
   * 指定均衡过程中可容忍的最大错误数。
   */
  public static final String MAXERROR = "maxerror";
  /**
   * 在目标DataNode上执行指定均衡计划命令。
   */
  public static final String EXECUTE = "execute";

  /**
   * 跳过计划过期检查，强制执行计划。默认计划生成后24小时内有效。
   */
  public static final String SKIPDATECHECK = "skipDateCheck";

  /**
   * 报告命令，输出集群磁盘数据分布不均报告。
   * 默认输出数据密度偏差最大的TOP N节点，这些节点最需要进行数据均衡。
   */
  public static final String REPORT = "report";
  /**
   * 指定报告要显示的不均衡节点数量。
   */
  public static final String TOP = "top";
  /**
   * 默认报告显示的不均衡节点数量。
   */
  public static final int DEFAULT_TOP = 100;
  /**
   * 指定要操作的目标DataNode地址或名称。
   */
  public static final String NODE = "node";
  /**
   *  verbose模式，输出更详细的执行日志。
   */
  public static final String VERBOSE = "v";
  public static final int PLAN_VERSION = 1;
  /**
   * 查询命令，查询磁盘均衡操作的当前状态。
   */
  public static final String QUERY = "query";
  /**
   * 取消正在运行的均衡计划命令。
   */
  public static final String CANCEL = "cancel";
  /**
   * 均衡前状态信息文件命名模板。
   */
  public static final String BEFORE_TEMPLATE = "%s.before.json";
  /**
   * 均衡计划文件命名模板。
   */
  public static final String PLAN_TEMPLATE = "%s.plan.json";
  private static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerCLI.class);

  private static final Options PLAN_OPTIONS = new Options();
  private static final Options EXECUTE_OPTIONS = new Options();
  private static final Options QUERY_OPTIONS = new Options();
  private static final Options HELP_OPTIONS = new Options();
  private static final Options CANCEL_OPTIONS = new Options();
  private static final Options REPORT_OPTIONS = new Options();

  private final PrintStream printStream;

  private Command currentCommand = null;

  /**
   * 构造DiskBalancerCLI实例，使用默认输出流。
   *
   * @param conf HDFS配置对象
   */
  public DiskBalancerCLI(Configuration conf) {
    this(conf, System.out);
  }

  /**
   * 构造DiskBalancerCLI实例，指定配置和输出流。
   *
   * @param conf HDFS配置对象
   * @param printStream 输出流对象
   */
  public DiskBalancerCLI(Configuration conf, final PrintStream printStream) {
    super(conf);
    this.printStream = printStream;
  }

  /**
   * 磁盘均衡器命令行入口方法。
   *
   * @param argv 命令行参数数组
   * @throws Exception 执行过程中抛出的异常
   */
  public static void main(String[] argv) throws Exception {
    DiskBalancerCLI shell = new DiskBalancerCLI(new HdfsConfiguration());
    int res = 0;
    try {
      res = ToolRunner.run(shell, argv);
    } catch (Exception ex) {
      String msg = String.format("Exception thrown while running %s.",
          DiskBalancerCLI.class.getSimpleName());
      LOG.error(msg, ex);
      res = 1;
    }
    System.exit(res);
  }

  /**
   * Tool接口实现方法，解析命令行参数并分发命令。
   *
   * @param args 命令行参数数组
   * @return 执行结果退出码，0表示成功，非0表示失败
   * @throws Exception 执行过程中抛出的异常
   */
  @Override
  public int run(String[] args) throws Exception {
    Options opts = getOpts();
    CommandLine cmd = parseArgs(args, opts);
    String[] cmdArgs = cmd.getArgs();
    // 检查参数数量是否合法
    if (cmdArgs.length > 2) {
      throw new HadoopIllegalArgumentException(
          "Invalid or extra Arguments: " + Arrays
              .toString(Arrays.copyOfRange(cmdArgs, 2, cmdArgs.length)));
    }
    return dispatch(cmd);
  }

  /**
   * 初始化并返回所有支持的命令行选项。
   *
   * @return 完整命令行选项集合
   */
  private Options getOpts() {
    Options opts = new Options();
    addPlanCommands(opts);
    addHelpCommands(opts);
    addExecuteCommands(opts);
    addQueryCommands(opts);
    addCancelCommands(opts);
    addReportCommands(opts);
    return opts;
  }

  /**
   * 获取plan命令支持的选项集合。
   *
   * @return plan命令选项集合
   */
  public static Options getPlanOptions() {
    return PLAN_OPTIONS;
  }

  /**
   * 获取help命令支持的选项集合。
   *
   * @return help命令选项集合
   */
  public static Options getHelpOptions() {
    return HELP_OPTIONS;
  }

  /**
   * 获取execute命令支持的选项集合。
   *
   * @return execute命令选项集合
   */
  public static Options getExecuteOptions() {
    return EXECUTE_OPTIONS;
  }

  /**
   * 获取query命令支持的选项集合。
   *
   * @return query命令选项集合
   */
  public static Options getQueryOptions() {
    return QUERY_OPTIONS;
  }

  /**
   * 获取cancel命令支持的选项集合。
   *
   * @return cancel命令选项集合
   */
  public static Options getCancelOptions() {
    return CANCEL_OPTIONS;
  }

  /**
   * 获取report命令支持的选项集合。
   *
   * @return report命令选项集合
   */
  public static Options getReportOptions() {
    return REPORT_OPTIONS;
  }

  /**
   * 向选项集合中添加plan命令相关选项。
   *
   * @param opt 全局选项集合
   */
  private void addPlanCommands(Options opt) {

    Option plan = Option.builder().longOpt(PLAN)
        .desc("Hostname, IP address or UUID of datanode " +
            "for which a plan is created.")
        .hasArg()
        .build();
    getPlanOptions().addOption(plan);
    opt.addOption(plan);


    Option outFile = Option.builder().longOpt(OUTFILE).hasArg()
        .desc(
            "Local path of file to write output to, if not specified "
                + "defaults will be used.")
        .build();
    getPlanOptions().addOption(outFile);
    opt.addOption(outFile);

    Option bandwidth = Option.builder().longOpt(BANDWIDTH).hasArg()
        .desc(
            "Maximum disk bandwidth (MB/s) in integer to be consumed by "
                + "diskBalancer. e.g. 10 MB/s.")
        .build();
    getPlanOptions().addOption(bandwidth);
    opt.addOption(bandwidth);

    Option threshold = Option.builder().longOpt(THRESHOLD)
        .hasArg()
        .desc("Percentage of data skew that is tolerated before"
            + " disk balancer starts working. For example, if"
            + " total data on a 2 disk node is 100 GB then disk"
            + " balancer calculates the expected value on each disk,"
            + " which is 50 GB. If the tolerance is 10% then data"
            + " on a single disk needs to be more than 60 GB"
            + " (50 GB + 10% tolerance value) for Disk balancer to"
            + " balance the disks.")
        .build();
    getPlanOptions().addOption(threshold);
    opt.addOption(threshold);


    Option maxError = Option.builder().longOpt(MAXERROR)
        .hasArg()
        .desc("Describes how many errors " +
            "can be tolerated while copying between a pair of disks.")
        .build();
    getPlanOptions().addOption(maxError);
    opt.addOption(maxError);

    Option verbose = Option.builder().longOpt(VERBOSE)
        .desc("Print out the summary of the plan on console")
        .build();
    getPlanOptions().addOption(verbose);
    opt.addOption(verbose);
  }

  /**
   * 向选项集合中添加help命令相关选项。
   *
   * @param opt 全局选项集合
   */
  private void addHelpCommands(Options opt) {
    Option help =  Option.builder().longOpt(HELP)
        .optionalArg(true)
        .desc("valid commands are plan | execute | query | cancel" +
            " | report")
        .build();
    getHelpOptions().addOption(help);
    opt.addOption(help);
  }

  /**
   * 向选项集合中添加execute命令相关选项。
   *
   * @param opt 全局选项集合
   */
  private void addExecuteCommands(Options opt) {
    Option execute = Option.builder().longOpt(EXECUTE)
        .hasArg()
        .desc("Takes a plan file and " +
            "submits it for execution by the datanode.")
        .build();
    getExecuteOptions().addOption(execute);


    Option skipDateCheck = Option.builder().longOpt(SKIPDATECHECK)
        .desc("skips the date check and force execute the plan")
        .build();
    getExecuteOptions().addOption(skipDateCheck);

    opt.addOption(execute);
    opt.addOption(skipDateCheck);
  }

  /**
   * 向选项集合中添加query命令相关选项。
   *
   * @param opt 全局选项集合
   */
  private void addQueryCommands(Options opt) {
    Option query = Option.builder().longOpt(QUERY)
        .hasArg()
        .desc("Queries the disk balancer " +
            "status of given datanode(s).")
        .build();
    getQueryOptions().addOption(query);
    opt.addOption(query);

    // Please note: Adding this only to Query options since -v is already
    // added to global table.
    Option verbose = Option.builder().longOpt(VERBOSE)
        .desc("Prints details of the plan that is being executed " +
            "on the datanode(s).")
        .build();
    getQueryOptions().addOption(verbose);
  }

  /**
   * 向选项集合中添加cancel命令相关选项。
   *
   * @param opt 全局选项集合
   */
  private void addCancelCommands(Options opt) {
    Option cancel = Option.builder().longOpt(CANCEL)
        .hasArg()
        .desc("Cancels a running plan using a plan file.")
        .build();
    getCancelOptions().addOption(cancel);
    opt.addOption(cancel);

    Option node = Option.builder().longOpt(NODE)
        .hasArg()
        .desc("Cancels a running plan using a plan ID and hostName")
        .build();

    getCancelOptions().addOption(node);
    opt.addOption(node);
  }

  /**
   * 向选项集合中添加report命令相关选项。
   *
   * @param opt 全局选项集合
   */
  private void addReportCommands(Options opt) {
    Option report = Option.builder().longOpt(REPORT)
        .desc("List nodes that will benefit from running " +
            "DiskBalancer.")
        .build();
    getReportOptions().addOption(report);
    opt.addOption(report);

    Option top = Option.builder().longOpt(TOP)
        .hasArg()
        .desc("specify the number of nodes to be listed which has" +
            " data imbalance.")
        .build();
    getReportOptions().addOption(top);
    opt.addOption(top);

    Option node =  Option.builder().longOpt(NODE)
        .hasArg()
        .desc("Datanode address, " +
            "it can be DataNodeID, IP or hostname.")
        .build();
    getReportOptions().addOption(node);
    opt.addOption(node);
  }

  /**
   * 解析命令行参数为CommandLine对象。
   *
   * @param argv 命令行参数数组
   * @param opts 支持的选项集合
   * @return 解析后的CommandLine对象
   * @throws org.apache.commons.cli.ParseException 参数解析异常
   */
  private CommandLine parseArgs(String[] argv, Options opts)
      throws org.apache.commons.cli.ParseException {
    BasicParser parser = new BasicParser();
    return parser.parse(opts, argv);
  }

  /**
   * 获取当前实例正在处理的命令对象。
   *
   * @return 当前命令对象
   */
  public Command getCurrentCommand() {
    return currentCommand;
  }

  /**
   * 根据解析后的命令行分发到对应命令处理器执行。
   *
   * @param cmd 解析后的命令行对象
   * @return 执行结果退出码，0表示成功，1表示失败
   * @throws Exception 执行过程中抛出的异常
   */
  private int dispatch(CommandLine cmd)
      throws Exception {
    Command dbCmd = null;
    try {
      if (cmd.hasOption(DiskBalancerCLI.PLAN)) {
        dbCmd = new PlanCommand(getConf(), printStream);
      }

      if (cmd.hasOption(DiskBalancerCLI.EXECUTE)) {
        dbCmd = new ExecuteCommand(getConf());
      }

      if (cmd.hasOption(DiskBalancerCLI.QUERY)) {
        dbCmd = new QueryCommand(getConf(), this.printStream);
      }

      if (cmd.hasOption(DiskBalancerCLI.CANCEL)) {
        dbCmd = new CancelCommand(getConf());
      }

      if (cmd.hasOption(DiskBalancerCLI.REPORT)) {
        dbCmd = new ReportCommand(getConf(), this.printStream);
      }

      if (cmd.hasOption(DiskBalancerCLI.HELP)) {
        dbCmd = new HelpCommand(getConf());
      }

      //