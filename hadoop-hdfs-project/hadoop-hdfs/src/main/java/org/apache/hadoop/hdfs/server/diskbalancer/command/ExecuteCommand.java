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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 磁盘均衡器执行命令类，负责将生成的磁盘均衡计划提交到对应数据节点执行
 * 是磁盘均衡命令的具体实现，核心职责是读取计划文件并提交给目标数据节点执行均衡任务
 */
public class ExecuteCommand extends Command {

  /**
   * 构造执行命令对象，注册当前命令支持的参数选项
   * @param conf Hadoop配置对象
   */
  public ExecuteCommand(Configuration conf) {
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.EXECUTE,
        "Executes a given plan.");
    addValidCommandParameters(DiskBalancerCLI.SKIPDATECHECK,
        "skips the date check and force execute the plan");
  }

  /**
   * 执行命令的主入口，解析命令行参数，读取计划文件并提交到目标数据节点
   * @param cmd 命令行参数对象
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.info("Executing \"execute plan\" command");
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.EXECUTE));
    verifyCommandOptions(DiskBalancerCLI.EXECUTE, cmd);

    String planFile = cmd.getOptionValue(DiskBalancerCLI.EXECUTE);
    Preconditions.checkArgument(planFile != null && !planFile.isEmpty(),
        "Invalid plan file specified.");

    String planData = null;
    // 自动关闭流读取计划文件内容
    try (FSDataInputStream plan = open(planFile)) {
      planData = IOUtils.toString(plan, StandardCharsets.UTF_8);
    }

    boolean skipDateCheck = false;
    // 检查是否开启跳过日期检查选项
    if(cmd.hasOption(DiskBalancerCLI.SKIPDATECHECK)) {
      skipDateCheck = true;
      LOG.warn("Skipping date check on this plan. This could mean we are " +
          "executing an old plan and may not be the right plan for this " +
          "data node.");
    }

    submitPlan(planFile, planData, skipDateCheck);
  }

  /**
   * 将解析完成的均衡计划提交给目标数据节点执行，建立数据节点代理并提交任务
   * @param planFile 计划文件路径
   * @param planData JSON格式的计划数据
   * @param skipDateCheck 是否跳过计划日期检查
   * @throws IOException 读取计划或RPC调用失败抛出异常
   */
  private void submitPlan(final String planFile, final String planData,
                          boolean skipDateCheck)
          throws IOException {
    Preconditions.checkNotNull(planData);
    // 解析JSON格式的计划数据
    NodePlan plan = NodePlan.parseJson(planData);
    // 从计划中获取目标数据节点地址
    String dataNodeAddress = plan.getNodeName() + ":" + plan.getPort();
    Preconditions.checkNotNull(dataNodeAddress);
    // 获取目标数据节点的RPC代理对象
    ClientDatanodeProtocol dataNode = getDataNodeProxy(dataNodeAddress);
    // 计算计划数据的SHA1哈希，用于数据节点校验计划完整性
    String planHash = DigestUtils.sha1Hex(planData);
    try {
      // 向数据节点提交磁盘均衡计划
      dataNode.submitDiskBalancerPlan(planHash, DiskBalancerCLI.PLAN_VERSION,
                                      planFile, planData, skipDateCheck);
    } catch (DiskBalancerException ex) {
      LOG.error("Submitting plan on  {} failed. Result: {}, Message: {}",
          plan.getNodeName(), ex.getResult().toString(), ex.getMessage());
      throw ex;
    }
  }

  /**
   * 打印execute命令的扩展帮助信息，说明命令用法和参数说明
   */
  @Override
  public void printHelp() {
    String header = "Execute command runs a submits a plan for execution on " +
        "the given data node.\n\n";

    String footer = "\nExecute command submits the job to data node and " +
        "returns immediately. The state of job can be monitored via query " +
        "command. ";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -execute <planfile>",
        header, DiskBalancerCLI.getExecuteOptions(), footer);
  }
}