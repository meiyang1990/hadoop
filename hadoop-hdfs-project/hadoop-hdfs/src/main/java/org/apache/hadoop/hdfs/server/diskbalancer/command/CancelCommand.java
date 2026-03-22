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
 * 文件级：磁盘均衡器取消命令实现类，负责取消指定DataNode上正在运行的磁盘均衡计划。
 * 提供两种取消方式：通过计划文件读取信息取消，或直接通过节点地址和计划哈希取消。
 * Cancels a running plan.
 */
public class CancelCommand extends Command {
  /**
   * 构造取消命令对象，注册命令参数和描述信息。
   *
   * @param conf Hadoop配置对象
   */
  public CancelCommand(Configuration conf) {
    super(conf);
    addValidCommandParameters(DiskBalancerCLI.CANCEL,
        "Cancels a running plan.");
    addValidCommandParameters(DiskBalancerCLI.NODE,
        "Node to run the command against in node:port format.");
  }

  /**
   * 执行取消命令，根据输入参数选择取消方式。
   *
   * @param cmd 命令行参数对象
   * @throws Exception 执行过程中的异常
   */
  @Override
  public void execute(CommandLine cmd) throws Exception {
    LOG.info("Executing \"Cancel plan\" command.");
    Preconditions.checkState(cmd.hasOption(DiskBalancerCLI.CANCEL));
    verifyCommandOptions(DiskBalancerCLI.CANCEL, cmd);

    // We can cancel a plan using datanode address and plan ID
    // that you can read from a datanode using queryStatus
    if(cmd.hasOption(DiskBalancerCLI.NODE)) {
      // 节点地址参数存在，直接使用节点地址和计划哈希取消
      String nodeAddress = cmd.getOptionValue(DiskBalancerCLI.NODE);
      String planHash = cmd.getOptionValue(DiskBalancerCLI.CANCEL);
      cancelPlanUsingHash(nodeAddress, planHash);
    } else {
      // Or you can cancel a plan using the plan file. If the user
      // points us to the plan file, we can compute the hash as well as read
      // the address of the datanode from the plan file.
      // 节点地址参数不存在，通过计划文件读取信息后取消
      String planFile = cmd.getOptionValue(DiskBalancerCLI.CANCEL);
      Preconditions.checkArgument(planFile != null && !planFile.isEmpty(),
          "Invalid plan file specified.");
      String planData = null;
      try (FSDataInputStream plan = open(planFile)) {
        planData = IOUtils.toString(plan, StandardCharsets.UTF_8);
      }
      cancelPlan(planData);
    }
  }

  /**
   * 根据计划JSON数据解析信息，向目标DataNode发起取消请求。
   *
   * @param planData 计划JSON字符串数据
   * @throws IOException 读取计划或RPC调用异常
   */
  private void cancelPlan(String planData) throws IOException {
    Preconditions.checkNotNull(planData);
    // 解析JSON格式的计划数据
    NodePlan plan = NodePlan.parseJson(planData);
    // 从计划中拼接DataNode地址
    String dataNodeAddress = plan.getNodeName() + ":" + plan.getPort();
    Preconditions.checkNotNull(dataNodeAddress);
    // 获取DataNode的RPC代理
    ClientDatanodeProtocol dataNode = getDataNodeProxy(dataNodeAddress);
    // 计算计划数据的SHA-1哈希作为计划ID
    String planHash = DigestUtils.sha1Hex(planData);
    try {
      // 发起RPC调用取消计划
      dataNode.cancelDiskBalancePlan(planHash);
    } catch (DiskBalancerException ex) {
      LOG.error("Cancelling plan on  {} failed. Result: {}, Message: {}",
          plan.getNodeName(), ex.getResult().toString(), ex.getMessage());
      throw ex;
    }
  }

  /**
   * 使用节点地址和计划哈希直接向DataNode发起取消请求。
   * @param nodeAddress 目标DataNode地址，格式为节点:端口
   * @param hash 计划的SHA哈希，可通过查询状态命令从DataNode获取
   * @throws IOException RPC调用异常
   */
  private void cancelPlanUsingHash(String nodeAddress, String hash) throws
      IOException {
    Preconditions.checkNotNull(nodeAddress);
    Preconditions.checkNotNull(hash);
    // 获取DataNode的RPC代理
    ClientDatanodeProtocol dataNode = getDataNodeProxy(nodeAddress);
    try {
      // 发起RPC调用取消计划
      dataNode.cancelDiskBalancePlan(hash);
    } catch (DiskBalancerException ex) {
      LOG.error("Cancelling plan on  {} failed. Result: {}, Message: {}",
          nodeAddress, ex.getResult().toString(), ex.getMessage());
      throw ex;
    }
  }


  /**
   * 打印取消命令的帮助信息，包含使用说明和示例。
   */
  @Override
  public void printHelp() {
    String header = "Cancel command cancels a running disk balancer operation" +
        ".\n\n";

    String footer = "\nCancel command can be run via pointing to a plan file," +
        " or by reading the plan ID using the query command and then using " +
        "planID and hostname. Examples of how to run this command are \n" +
        "hdfs diskbalancer -cancel <planfile> \n" +
        "hdfs diskbalancer -cancel <planID> -node <hostname>";

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("hdfs diskbalancer -cancel <planFile> | -cancel " +
        "<planID> -node <hostname>",
        header, DiskBalancerCLI.getCancelOptions(), footer);
  }
}