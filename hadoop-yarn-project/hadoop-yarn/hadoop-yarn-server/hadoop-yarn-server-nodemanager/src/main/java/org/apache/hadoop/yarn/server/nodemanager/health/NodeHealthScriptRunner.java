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

package org.apache.hadoop.yarn.server.nodemanager.health;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 节点健康检查脚本运行器，通过执行用户配置的外部健康检查脚本定期检测NodeManager节点健康状态，并将结果上报给健康检查服务
 */
public class NodeHealthScriptRunner extends TimedHealthReporterService {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeHealthScriptRunner.class);

  /** 健康检查脚本的绝对路径 */
  private String nodeHealthScript;
  /** 脚本执行超时时间 */
  private long scriptTimeout;
  /** 用于执行监控脚本的Shell命令执行器 */
  private ShellCommandExecutor commandExecutor = null;

  /** 健康检查脚本输出中错误标识的匹配模式：行首为ERROR */
  private static final String ERROR_PATTERN = "ERROR";

  /** 脚本超时错误提示信息 */
  static final String NODE_HEALTH_SCRIPT_TIMED_OUT_MSG =
      "Node health script timed out";

  /**
   * 私有构造函数，创建节点健康检查脚本运行器实例
   * @param scriptName 健康检查脚本路径
   * @param checkInterval 健康检查间隔时间（毫秒）
   * @param timeout 脚本执行超时时间（毫秒）
   * @param scriptArgs 脚本执行参数数组
   * @param runBeforeStartup 是否在NodeManager启动前执行首次检查
   */
  private NodeHealthScriptRunner(String scriptName, long checkInterval,
      long timeout, String[] scriptArgs, boolean runBeforeStartup) {
    super(NodeHealthScriptRunner.class.getName(), checkInterval,
        runBeforeStartup);
    this.nodeHealthScript = scriptName;
    this.scriptTimeout = timeout;
    setTimerTask(new NodeHealthMonitorExecutor(scriptArgs));
  }

  /**
   * 根据配置创建节点健康检查脚本运行器实例
   * @param scriptName 健康检查脚本名称
   * @param conf Yarn配置对象
   * @return 创建完成的运行器实例，如果配置不合法则返回null
   */
  public static NodeHealthScriptRunner newInstance(String scriptName,
      Configuration conf) {
    // 从配置中读取脚本路径
    String nodeHealthScriptsConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH_TEMPLATE, scriptName);
    String nodeHealthScript = conf.get(nodeHealthScriptsConfig);
    if (!shouldRun(scriptName, nodeHealthScript)) {
      return null;
    }

    // 读取并计算健康检查间隔（毫秒）
    String checkIntervalMsConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_INTERVAL_MS_TEMPLATE,
        scriptName);
    long checkIntervalMs = conf.getLong(checkIntervalMsConfig, 0L);
    if (checkIntervalMs == 0L) {
      // 若未配置脚本专属间隔，使用全局默认间隔
      checkIntervalMs = conf.getLong(
          YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS,
          YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS);
    }
    if (checkIntervalMs < 0) {
      throw new IllegalArgumentException("The node health-checker's " +
          "interval-ms can not be set to a negative number.");
    }

    // 读取是否在NodeManager启动前执行检查的配置
    boolean runBeforeStartup = conf.getBoolean(
        YarnConfiguration.NM_HEALTH_CHECK_RUN_BEFORE_STARTUP,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_RUN_BEFORE_STARTUP);

    // 读取并计算脚本超时时间
    String scriptTimeoutConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS_TEMPLATE,
        scriptName);
    long scriptTimeout = conf.getLong(scriptTimeoutConfig, 0L);
    if (scriptTimeout == 0L) {
      // 若未配置脚本专属超时，使用全局默认超时
      scriptTimeout = conf.getLong(
          YarnConfiguration.NM_HEALTH_CHECK_TIMEOUT_MS,
          YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_TIMEOUT_MS);
    }
    if (scriptTimeout <= 0) {
      throw new IllegalArgumentException("The node health-checker's " +
          "timeout can only be set to a positive number.");
    }

    // 读取脚本执行参数
    String scriptArgsConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_OPTS_TEMPLATE,
        scriptName);
    String[] scriptArgs = conf.getStrings(scriptArgsConfig, new String[]{});

    return new NodeHealthScriptRunner(nodeHealthScript,
        checkIntervalMs, scriptTimeout, scriptArgs, runBeforeStartup);
  }

  /**
   * 健康检查执行结果状态枚举
   */
  private enum HealthCheckerExitStatus {
    /** 检查成功，节点健康 */
    SUCCESS,
    /** 脚本执行超时 */
    TIMED_OUT,
    /** 脚本执行返回非零退出码 */
    FAILED_WITH_EXIT_CODE,
    /** 脚本执行抛出异常 */
    FAILED_WITH_EXCEPTION,
    /** 脚本输出包含ERROR信息 */
    FAILED
  }


  /**
   * 定时任务类，由Timer调度定期执行外部健康检查脚本
   */
  private class NodeHealthMonitorExecutor extends TimerTask {
    private String exceptionStackTrace = "";

    /**
     * 构造执行器，组装脚本命令
     * @param args 脚本执行参数
     */
    NodeHealthMonitorExecutor(String[] args) {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(nodeHealthScript);
      if (args != null) {
        execScript.addAll(Arrays.asList(args));
      }
      commandExecutor = new ShellCommandExecutor(execScript
          .toArray(new String[execScript.size()]), null, null, scriptTimeout);
    }

    @Override
    public void run() {
      HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
      try {
        // 执行健康检查脚本
        commandExecutor.execute();
      } catch (ExitCodeException e) {
        // 脚本返回非零退出码，默认标记为失败（Windows平台特殊处理超时判断）
        status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
        // Windows平台需要额外判断是否超时
        if (Shell.WINDOWS && commandExecutor.isTimedOut()) {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
      } catch (Exception e) {
        LOG.warn("Caught exception : " + e.getMessage());
        // 根据是否超时标记对应状态
        if (!commandExecutor.isTimedOut()) {
          status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
        } else {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
        exceptionStackTrace = StringUtils.stringifyException(e);
      } finally {
        // 如果执行成功，检查输出中是否包含ERROR行
        if (status == HealthCheckerExitStatus.SUCCESS) {
          if (hasErrors(commandExecutor.getOutput())) {
            status = HealthCheckerExitStatus.FAILED;
          }
        }
        // 根据检查结果上报节点健康状态
        reportHealthStatus(status);
      }
    }

    /**
     * 根据检查结果更新节点健康状态并上报
     * @param status 健康检查结果状态
     */
    void reportHealthStatus(HealthCheckerExitStatus status) {
      switch (status) {
      case SUCCESS:
      case FAILED_WITH_EXIT_CODE:
        // 成功或脚本非零退出码不标记节点不健康，遵循文档约定
        setHealthyWithoutReport();
        break;
      case TIMED_OUT:
        // 脚本超时，标记节点不健康并上报超时信息
        setUnhealthyWithReport(NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
        break;
      case FAILED_WITH_EXCEPTION:
        // 执行异常，标记节点不健康并上报异常栈
        setUnhealthyWithReport(exceptionStackTrace);
        break;
      case FAILED:
        // 输出包含ERROR，标记节点不健康并上报脚本完整输出
        setUnhealthyWithReport(commandExecutor.getOutput());
        break;
      default:
        LOG.warn("Unknown HealthCheckerExitStatus - ignored.");
        break;
      }
    }

    /**
     * 检查脚本输出是否包含以ERROR开头的行，判断是否存在健康问题
     * @param output 健康检查脚本输出
     * @return true 存在错误，false 无错误
     */
    private boolean hasErrors(String output) {
      String[] splits = output.split("\n");
      for (String split : splits) {
        if (split.startsWith(ERROR_PATTERN)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void serviceStop() throws Exception {
    // 服务停止时销毁仍在运行的脚本进程
    if (commandExecutor != null) {
      Process p = commandExecutor.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
    super.serviceStop();
  }

  /**
   * 检查健康检查脚本配置是否合法、文件是否存在且可执行，判断是否需要启动该脚本检查
   * @param script 脚本名称
   * @param healthScript 脚本路径
   * @return true 可以启动检查，false 不启动
   */
  static boolean shouldRun(String script, String healthScript) {
    if (healthScript == null || healthScript.trim().isEmpty()) {
      LOG.info("Missing location for the node health check script \"{}\".",
          script);
      return false;
    }
    File f = new File(healthScript);
    if (!f.exists()) {
      LOG.warn("File {} for script \"{}\" does not exist.",
          healthScript, script);
      return false;
    }
    if (!FileUtil.canExecute(f)) {
      LOG.warn("File {} for script \"{}\" can not be executed.",
          healthScript, script);
      return false;
    }
    return true;
  }
}