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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.FailoverFailedException;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocolHelper;
import org.apache.hadoop.ha.ServiceFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * DFSHAAdmin是HAAdmin的扩展，实现HDFS高可用特有的管理功能，
 * 提供命令行工具管理HDFS NameNode高可用切换、状态转换等操作。
 */
public class DFSHAAdmin extends HAAdmin {

  private static final String FORCEFENCE  = "forcefence";
  private static final Logger LOG = LoggerFactory.getLogger(DFSHAAdmin.class);

  private String nameserviceId;
  // HDFS特有命令使用信息表
  private final static Map<String, UsageInfo> USAGE_DFS_ONLY =
      ImmutableMap.<String, UsageInfo> builder()
          .put("-transitionToObserver", new UsageInfo("<serviceId>",
                  "Transitions the service into Observer state"))
          .put("-failover", new UsageInfo(
              "[--"+FORCEFENCE+"] [--"+FORCEACTIVE+"] "
                  + "<serviceId> <serviceId>",
              "Failover from the first service to the second.\n"
                  + "Unconditionally fence services if the --" + FORCEFENCE
                  + " option is used.\n"
                  + "Try to failover to the target service "
                  + "even if it is not ready if the "
                  + "--" + FORCEACTIVE + " option is used.")).build();

  // 合并通用HA命令和HDFS特有命令后的完整使用信息表
  private final static Map<String, UsageInfo> USAGE_DFS_MERGED =
      ImmutableSortedMap.<String, UsageInfo> naturalOrder()
          .putAll(USAGE)
          .putAll(USAGE_DFS_ONLY)
          .build();

  /**
   * 设置错误输出流，用于测试时重定向错误输出。
   * @param errOut 错误输出流
   */
  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }
  
  /**
   * 设置标准输出流，用于测试时重定向标准输出。
   * @param out 标准输出流
   */
  protected void setOut(PrintStream out) {
    this.out = out;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      // 添加HDFS安全相关配置
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  /**
   * 为配置添加HDFS NameNode安全认证配置，加载HDFS配置文件并返回新配置副本。
   * @param conf 原始配置对象
   * @return 添加了安全配置后的新配置对象
   */
  public static Configuration addSecurityConfiguration(Configuration conf) {
    // 创建配置副本并强制加载hdfs-site.xml，避免修改原配置
    conf = new HdfsConfiguration(conf);
    // 从配置中获取NameNode Kerberos主体
    String nameNodePrincipal = conf.get(
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using NN principal: " + nameNodePrincipal);
    }
    // 将NameNode主体设置到Hadoop安全服务用户配置项
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        nameNodePrincipal);
    return conf;
  }

  /**
   * 根据给定的NameNode ID解析获取对应HA服务目标对象，用于HDFS NameNode服务寻址。
   * @param nnId NameNode ID
   * @return HDFS NameNode HA服务目标对象
   */
  @Override
  protected HAServiceTarget resolveTarget(String nnId) {
    HdfsConfiguration conf = (HdfsConfiguration)getConf();
    return new NNHAServiceTarget(conf, nameserviceId, nnId);
  }

  @Override
  protected String getUsageString() {
    // 返回带nameservice参数的使用说明前缀
    return "Usage: haadmin [-ns <nameserviceId>]";
  }

  /**
   * 为failover命令添加专属CLI选项。
   * @param failoverOpts 选项对象，用于添加failover专属选项
   */
  private void addFailoverCliOpts(Options failoverOpts) {
    failoverOpts.addOption(FORCEFENCE, false, "force fencing");
    failoverOpts.addOption(FORCEACTIVE, false, "force failover");
    // FORCEMANUAL由上层统一为所有变更状态的命令添加，此处不重复添加
  }

  @Override
  protected boolean checkParameterValidity(String[] argv){
    return  checkParameterValidity(argv, USAGE_DFS_MERGED);
  }

  @Override
  protected int runCmd(String[] argv) throws Exception {

    if(argv.length < 1){
      printUsage(errOut, USAGE_DFS_MERGED);
      return -1;
    }

    int i = 0;
    String cmd = argv[i++];
    // 处理-nameserviceId选项
    if ("-ns".equals(cmd)) {
      if (i == argv.length) {
        errOut.println("Missing nameservice ID");
        printUsage(errOut, USAGE_DFS_MERGED);
        return -1;
      }
      // 保存指定的nameservice ID
      nameserviceId = argv[i++];
      if (i >= argv.length) {
        errOut.println("Missing command");
        printUsage(errOut, USAGE_DFS_MERGED);
        return -1;
      }
      // 裁剪参数数组，去掉已经处理的-ns和nameserviceId
      argv = Arrays.copyOfRange(argv, i, argv.length);
      cmd = argv[0];
    }

    if (!checkParameterValidity(argv)){
      return -1;
    }

    /*
       -help命令需要在这里处理，因为HAAdmin和DFSHAAdmin都需要支持，
       帮助信息包含HDFS特有命令，因此需要使用合并后的使用信息表
    */
    if ("-help".equals(cmd)){
      return help(argv, USAGE_DFS_MERGED);
    }

    // 如果不是HDFS特有命令，交给父类处理
    if (!USAGE_DFS_ONLY.containsKey(cmd)) {
      return super.runCmd(argv);
    }

    Options opts = new Options();
    // 为failover命令添加专属选项
    if ("-failover".equals(cmd)) {
      addFailoverCliOpts(opts);
    }
    // 变更状态的命令都需要添加FORCEMANUAL选项
    if ("-transitionToObserver".equals(cmd) ||
        "-failover".equals(cmd)) {
      opts.addOption(FORCEMANUAL, false,
          "force manual control even if auto-failover is enabled");
    }
    // 解析命令行参数
    CommandLine cmdLine = parseOpts(cmd, opts, argv, USAGE_DFS_MERGED);
    if (cmdLine == null) {
      return -1;
    }

    // 如果指定了FORCEMANUAL，需要用户确认并设置强制请求源
    if (cmdLine.hasOption(FORCEMANUAL)) {
      if (!confirmForceManual()) {
        LOG.error("Aborted");
        return -1;
      }
      // 即使配置了自动failover，仍然强制让NameNode响应用户请求
      setRequestSource(RequestSource.REQUEST_BY_USER_FORCED);
    }

    // 分发处理不同HDFS特有命令
    if ("-transitionToObserver".equals(cmd)) {
      return transitionToObserver(cmdLine);
    } else if ("-failover".equals(cmd)) {
      return failover(cmdLine);
    } else {
      // 逻辑上不会走到这里，抛出断言错误方便调试
      throw new AssertionError("Should not get here, command: " + cmd);
    }
  }
  
  /**
   * 获取当前配置下所有NameNode ID列表，用于命令补全和参数校验。
   * @param namenodeToActivate 待激活的NameNode（此处未使用）
   * @return 当前nameservice下所有NameNode ID集合
   */
  @Override
  protected Collection<String> getTargetIds(String namenodeToActivate) {
    return DFSUtilClient.getNameNodeIds(
        getConf(), (nameserviceId != null)?
            nameserviceId : DFSUtil.getNamenodeNameServiceId(getConf()));
  }

  /**
   * 检查目标服务是否支持Observer状态。
   * @param target 待检查的HA服务目标
   * @return true表示支持，false表示不支持
   */
  private boolean checkSupportObserver(HAServiceTarget target) {
    if (target.supportObserver()) {
      return true;
    } else {
      errOut.println(
          "The target " + target + " doesn't support Observer state.");
      return false;
    }
  }

  /**
   * 执行transitionToObserver命令，将指定NameNode转换为Observer状态。
   * @param cmd 解析后的命令行对象
   * @return 执行结果，0成功，-1失败
   * @throws IOException 执行过程中IO异常
   */
  private int transitionToObserver(final CommandLine cmd)
      throws IOException {
    String[] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("transitionToObserver: incorrect number of arguments");
      printUsage(errOut, "-transitionToObserver", USAGE_DFS_MERGED);
      return -1;
    }
    // 解析目标NameNode
    HAServiceTarget target = resolveTarget(argv[0]);
    if (!checkSupportObserver(target)) {
      return -1;
    }
    if (!checkManualStateManagementOK(target)) {
      return -1;
    }
    try {
      // 获取目标服务代理，执行状态转换
      HAServiceProtocol proto = target.getProxy(getConf(), 0);
      HAServiceProtocolHelper.transitionToObserver(proto, createReqInfo());
    } catch (ServiceFailedException e) {
      errOut.println("transitionToObserver failed! " + e.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  /**
   * 执行手动failover命令，将Active状态从第一个NameNode切换到第二个NameNode。
   * @param cmd 解析后的命令行对象
   * @return 执行结果，0成功，-1失败
   * @throws IOException 执行过程中IO异常
   * @throws ServiceFailedException 服务操作异常
   */
  private int failover(CommandLine cmd)
      throws IOException, ServiceFailedException {
    // 从命令行获取强制fence和强制激活选项
    boolean forceFence = cmd.hasOption(FORCEFENCE);
    boolean forceActive = cmd.hasOption(FORCEACTIVE);

    int numOpts = cmd.getOptions() == null ? 0 : cmd.getOptions().length;
    final String[] args = cmd.getArgs();

    // 参数校验：最多3个选项，必须正好两个服务参数
    if (numOpts > 3 || args.length != 2) {
      errOut.println("failover: incorrect arguments");
      printUsage(errOut, "-failover", USAGE_DFS_MERGED);
      return -1;
    }
    // 解析源和目标NameNode
    HAServiceTarget fromNode = resolveTarget(args[0]);
    HAServiceTarget toNode = resolveTarget(args[1]);
    // 设置期望转换后的目标状态
    fromNode.setTransitionTargetHAStatus(
        HAServiceProtocol.HAServiceState.STANDBY);
    toNode.setTransitionTargetHAStatus(
        HAServiceProtocol.HAServiceState.ACTIVE);

    // 校验两个节点自动failover配置必须一致
    Preconditions.checkState(
        fromNode.isAutoFailoverEnabled() ==
            toNode.isAutoFailoverEnabled(),
        "Inconsistent auto-failover configs between %s and %s!",
        fromNode, toNode);

    // 自动failover开启场景下，不支持手动强制参数
    if (fromNode.isAutoFailoverEnabled()) {
      if (forceFence || forceActive) {
        errOut.println(FORCEFENCE + " and " + FORCEACTIVE + " flags not " +
            "supported with auto-failover enabled.");
        return -1;
      }
      try {
        // 通过ZKFC执行优雅failover
        return gracefulFailoverThroughZKFCs(toNode);
      } catch (UnsupportedOperationException e){
        errOut.println("Failover command is not supported with " +
            "auto-failover enabled: " + e.getLocalizedMessage());
        return -1;
      }
    }

    // 手动failover场景，创建failover控制器执行切换
    FailoverController fc =
        new FailoverController(getConf(), getRequestSource());

    try {
      fc.failover(fromNode, toNode, forceFence, forceActive);
      out.println("Failover from "+args[0]+" to "+args[1]+" successful");
    } catch (FailoverFailedException ffe) {
      errOut.println("Failover failed: " + ffe.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  /**
   * DFSHAAdmin命令行工具入口方法。
   * @param argv 命令行参数数组
   * @throws Exception 执行过程中抛出异常
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new DFSHAAdmin(), argv);
    System.exit(res);
  }
}