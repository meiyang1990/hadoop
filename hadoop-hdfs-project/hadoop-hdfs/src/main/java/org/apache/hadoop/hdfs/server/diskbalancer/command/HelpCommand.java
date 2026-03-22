// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import org.apache.hadoop.util.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;

/**
 * 磁盘均衡器帮助命令类，用于打印所有命令或特定命令的帮助信息。
 * 负责响应用户的-help请求，输出清晰的使用说明。
 */
public class HelpCommand extends Command {

  /**
   * 构造帮助命令对象，注册命令参数。
   *
   * @param conf Hadoop配置对象
   */
  public HelpCommand(Configuration conf) {
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.HELP, "Help Command");
  }

  /**
   * 执行帮助命令，根据用户输入输出对应帮助信息。
   * 如果用户未指定具体命令，输出整体帮助；如果指定了具体命令，输出对应命令的帮助。
   *
   * @param cmd 命令行参数对象
   * @throws Exception 执行过程中可能抛出的异常
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.debug("Processing help Command.");
    // 如果命令行对象为空，输出整体帮助
    if (cmd == null) {
      this.printHelp();
      return;
    }

    // 校验命令参数合法性
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.HELP));
    verifyCommandOptions(DiskBalancerCLI.HELP, cmd);
    // 获取用户请求帮助的具体命令名称
    String helpCommand = cmd.getOptionValue(DiskBalancerCLI.HELP);
    // 如果未指定具体命令，输出整体帮助
    if (helpCommand == null || helpCommand.isEmpty()) {
      this.printHelp();
      return;
    }

    // 格式化命令名称，统一去除空格转小写
    helpCommand = helpCommand.trim();
    helpCommand = helpCommand.toLowerCase();
    Command command = null;
    // 根据命令名称匹配对应命令对象
    switch (helpCommand) {
    case DiskBalancerCLI.PLAN:
      command = new PlanCommand(getConf());
      break;
    case DiskBalancerCLI.EXECUTE:
      command = new ExecuteCommand(getConf());
      break;
    case DiskBalancerCLI.QUERY:
      command = new QueryCommand(getConf());
      break;
    case DiskBalancerCLI.CANCEL:
      command = new CancelCommand(getConf());
      break;
    case DiskBalancerCLI.REPORT:
      command = new ReportCommand(getConf());
      break;
    default:
      // 未知命令默认输出当前帮助
      command = this;
      break;
    }
    // 输出对应命令的帮助信息
    command.printHelp();

  }

  /**
   * 打印磁盘均衡器整体帮助信息，说明工具用途和使用方式。
   */
  @Override
  public void printHelp() {
    String header = "\nDiskBalancer distributes data evenly between " +
        "different disks on a datanode. " +
        "DiskBalancer operates by generating a plan, that tells datanode " +
        "how to move data between disks. Users can execute a plan by " +
        "submitting it to the datanode. \nTo get specific help on a " +
        "particular command please run \n\n hdfs diskbalancer -help <command>.";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer [command] [options]",
        header, DiskBalancerCLI.getHelpOptions(), "");
  }


}