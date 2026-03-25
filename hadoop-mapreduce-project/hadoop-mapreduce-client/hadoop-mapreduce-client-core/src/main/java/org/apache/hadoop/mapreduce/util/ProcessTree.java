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

package org.apache.hadoop.mapreduce.util;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件描述：提供Linux系统进程树相关操作的工具类，主要用于MapReduce任务执行后清理子进程树，
 * 支持优雅终止（SIGTERM）后强制杀死（SIGKILL）的两步处理机制，支持单个进程和进程组两种模式。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProcessTree {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessTree.class);

  /** 默认发送SIGTERM后等待SIGKILL的间隔时间，单位毫秒 */
  public static final long DEFAULT_SLEEPTIME_BEFORE_SIGKILL = 5000L;

  private static final int SIGQUIT = 3;
  private static final int SIGTERM = 15;
  private static final int SIGKILL = 9;

  private static final String SIGQUIT_STR = "SIGQUIT";
  private static final String SIGTERM_STR = "SIGTERM";
  private static final String SIGKILL_STR = "SIGKILL";

  /** 标记当前系统是否支持setsid命令，用于创建新会话进程组 */
  public static final boolean isSetsidAvailable = isSetsidSupported();

  /**
   * 检查当前系统是否支持setsid命令
   * @return 支持返回true，否则返回false
   */
  private static boolean isSetsidSupported() {
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = {"setsid", "bash", "-c", "echo $$"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // 处理退出码并打印日志
      LOG.info("setsid exited with exit code " + shexec.getExitCode());
    }
    return setsidSupported;
  }

  /**
   * 销毁以指定pid为根的整个进程树，支持单个进程或进程组两种模式
   * @param pid 要销毁的进程树/进程组的根进程ID
   * @param sleeptimeBeforeSigkill 发送SIGTERM后等待发送SIGKILL的间隔时间，单位毫秒
   * @param isProcessGroup 传入的pid是否为进程组ID
   * @param inBackground 是否使用后台线程异步执行销毁操作
   */
  public static void destroy(String pid, long sleeptimeBeforeSigkill,
                             boolean isProcessGroup, boolean inBackground) {
    if(isProcessGroup) {
      destroyProcessGroup(pid, sleeptimeBeforeSigkill, inBackground);
    }
    else {
      //TODO: 此处未来需要实现销毁整个子树，当前仅杀死根进程
      destroyProcess(pid, sleeptimeBeforeSigkill, inBackground);
    }
  }

  /**
   * 销毁单个进程，先发送SIGTERM再发送SIGKILL
   * @param pid 目标进程ID
   * @param sleeptimeBeforeSigkill 发送SIGTERM后等待发送SIGKILL的间隔时间，单位毫秒
   * @param inBackground 是否后台异步执行
   */
  protected static void destroyProcess(String pid, long sleeptimeBeforeSigkill,
                                    boolean inBackground) {
    terminateProcess(pid);
    sigKill(pid, false, sleeptimeBeforeSigkill, inBackground);
  }

  /**
   * 销毁整个进程组，先发送SIGTERM再发送SIGKILL
   * @param pgrpId 目标进程组ID
   * @param sleeptimeBeforeSigkill 发送SIGTERM后等待发送SIGKILL的间隔时间，单位毫秒
   * @param inBackground 是否后台异步执行
   */
  protected static void destroyProcessGroup(String pgrpId,
                       long sleeptimeBeforeSigkill, boolean inBackground) {
    terminateProcessGroup(pgrpId);
    sigKill(pgrpId, true, sleeptimeBeforeSigkill, inBackground);
  }

  /**
   * 向指定进程/进程组发送指定信号
   * @param pid 目标进程/进程组ID（进程组需以负号开头）
   * @param signalNum 信号编号
   * @param signalName 信号名称，用于日志输出
   */
  private static void sendSignal(String pid, int signalNum, String signalName) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-" + signalNum, pid };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command " + ioe);
    } finally {
      if (pid.startsWith("-")) {
        LOG.info("Sending signal to all members of process group " + pid
            + ": " + signalName + ". Exit code " + shexec.getExitCode());
      } else {
        LOG.info("Signaling process " + pid
            + " with " + signalName + ". Exit code " + shexec.getExitCode());
      }
    }
  }

  /**
   * 如果进程存活或者强制标记开启，则发送指定信号给目标进程
   * @param pid 目标进程ID
   * @param signalNum 信号编号
   * @param signalName 信号名称，用于日志
   * @param alwaysSignal 即使进程检测为不存活也要发送信号
   */
  private static void maybeSignalProcess(String pid, int signalNum,
      String signalName, boolean alwaysSignal) {
    // 如果不强制发送，且进程已经不存活则跳过
    if (alwaysSignal || ProcessTree.isAlive(pid)) {
      sendSignal(pid, signalNum, signalName);
    }
  }

  /**
   * 如果进程组存活或者强制标记开启，则发送指定信号给目标进程组
   * @param pgrpId 目标进程组ID
   * @param signalNum 信号编号
   * @param signalName 信号名称，用于日志
   * @param alwaysSignal 即使进程组检测为不存活也要发送信号
   */
  private static void maybeSignalProcessGroup(String pgrpId, int signalNum,
      String signalName, boolean alwaysSignal) {

    if (alwaysSignal || ProcessTree.isProcessGroupAlive(pgrpId)) {
      // 给进程组发信号需要将pid转为负数
      sendSignal("-" + pgrpId, signalNum, signalName);
    }
  }

  /**
   * 给指定进程发送SIGTERM信号，请求优雅退出
   * @param pid 目标进程ID
   */
  public static void terminateProcess(String pid) {
    maybeSignalProcess(pid, SIGTERM, SIGTERM_STR, true);
  }

  /**
   * 给指定进程组所有进程发送SIGTERM信号，请求优雅退出
   * @param pgrpId 目标进程组ID
   */
  public static void terminateProcessGroup(String pgrpId) {
    maybeSignalProcessGroup(pgrpId, SIGTERM, SIGTERM_STR, true);
  }

  /**
   * 在当前线程执行SIGKILL流程，等待指定间隔后强制杀死进程/进程组
   * @param pid 目标进程/进程组ID
   * @param isProcessGroup 是否为进程组
   * @param sleepTimeBeforeSigKill 发送SIGTERM后等待发送SIGKILL的间隔时间，单位毫秒
   */
  private static void sigKillInCurrentThread(String pid, boolean isProcessGroup,
      long sleepTimeBeforeSigKill) {
    // 如果是进程组，即使根进程退出也要杀死剩余子进程，因此无需检查存活
    if (isProcessGroup || ProcessTree.isAlive(pid)) {
      try {
        // 等待一段时间给进程清理资源
        Thread.sleep(sleepTimeBeforeSigKill);
      } catch (InterruptedException i) {
        LOG.warn("Thread sleep is interrupted.");
      }
      if(isProcessGroup) {
        killProcessGroup(pid);
      } else {
        killProcess(pid);
      }
    }  
  }

  /**
   * 触发SIGKILL杀死流程，支持同步或异步执行
   * @param pid 目标进程/进程组ID
   * @param isProcessGroup 是否为进程组
   * @param sleeptimeBeforeSigkill 发送SIGTERM后等待发送SIGKILL的间隔时间，单位毫秒
   * @param inBackground 是否后台异步执行
   */
  private static void sigKill(String pid, boolean isProcessGroup,
                        long sleeptimeBeforeSigkill, boolean inBackground) {

    if(inBackground) { // 使用独立后台线程执行杀死操作
      SigKillThread sigKillThread = new SigKillThread(pid, isProcessGroup,
                                                      sleeptimeBeforeSigkill);
      sigKillThread.setDaemon(true);
      sigKillThread.start();
    }
    else {
      sigKillInCurrentThread(pid, isProcessGroup, sleeptimeBeforeSigkill);
    }
  }

  /**
   * 给指定进程发送SIGKILL信号，强制终止进程
   * @param pid 目标进程ID
   */
  public static void killProcess(String pid) {
    maybeSignalProcess(pid, SIGKILL, SIGKILL_STR, false);
  }

  /**
   * 给指定进程发送SIGQUIT信号，触发Java进程输出线程栈转储
   * @param pid 目标进程ID
   */
  public static void sigQuitProcess(String pid) {
    maybeSignalProcess(pid, SIGQUIT, SIGQUIT_STR, false);
  }

  /**
   * 给指定进程组所有进程发送SIGKILL信号，强制终止整个进程组
   * @param pgrpId 目标进程组ID
   */
  public static void killProcessGroup(String pgrpId) {
    maybeSignalProcessGroup(pgrpId, SIGKILL, SIGKILL_STR, false);
  }

  /**
   * 给指定进程组所有进程发送SIGQUIT信号，触发所有Java进程输出线程栈转储
   * @param pgrpId 目标进程组ID
   */
  public static void sigQuitProcessGroup(String pgrpId) {
    maybeSignalProcessGroup(pgrpId, SIGQUIT, SIGQUIT_STR, false);
  }

  /**
   * 检查指定PID的进程是否存活，不处理PID回绕场景
   * @param pid 目标进程ID
   * @return 存活返回true，否则返回false
   */
  public static boolean isAlive(String pid) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-0", pid };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (ExitCodeException ee) {
      return false;
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command "
          + shexec.toString() + ioe);
      return false;
    }
    return (shexec.getExitCode() == 0 ? true : false);
  }

  /**
   * 检查指定ID的进程组是否有存活进程，不处理PID回绕场景
   * @param pgrpId 目标进程组ID
   * @return 有存活进程返回true，否则返回false
   */
  public static boolean isProcessGroupAlive(String pgrpId) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-0", "-"+pgrpId };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (ExitCodeException ee) {
      return false;
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command "
          + shexec.toString() + ioe);
      return false;
    }
    return (shexec.getExitCode() == 0 ? true : false);
  }

  /**
   * 后台线程类，用于异步执行进程树SIGKILL操作，避免阻塞主线程
   */
  static class SigKillThread extends SubjectInheritingThread {
    private String pid = null;
    private boolean isProcessGroup = false;

    private long sleepTimeBeforeSigKill = DEFAULT_SLEEPTIME_BEFORE_SIGKILL;

    /**
     * 构造后台SIGKILL线程
     * @param pid 目标进程/进程组ID
     * @param isProcessGroup 是否为进程组
     * @param interval 发送SIGTERM后等待SIGKILL的间隔时间
     */
    private SigKillThread(String pid, boolean isProcessGroup, long interval) {
      this.pid = pid;
      this.isProcessGroup = isProcessGroup;
      this.setName(this.getClass().getName() + "-" + pid);
      sleepTimeBeforeSigKill = interval;
    }

    public void work() {
      sigKillInCurrentThread(pid, isProcessGroup, sleepTimeBeforeSigKill);
    }
  }
}