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
package org.apache.hadoop.mapreduce.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryViewer;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MapReduce作业命令行工具，解析并处理用户提交的MapReduce CLI命令，提供作业提交、状态查询、作业管理等功能
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CLI extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(CLI.class);
  protected Cluster cluster;
  // 支持的任务状态集合
  private final Set<String> taskStates = new HashSet<String>(
              Arrays.asList("pending", "running", "completed", "failed", "killed"));
  // 支持的任务类型集合
  private static final Set<String> taskTypes = new HashSet<String>(
      Arrays.asList("MAP", "REDUCE"));
  
  /**
   * 空构造方法，创建CLI实例
   */
  public CLI() {
  }
  
  /**
   * 带配置的构造方法，创建CLI实例并设置配置
   * @param conf Hadoop配置
   */
  public CLI(Configuration conf) {
    setConf(conf);
  }
  
  /**
   * CLI工具主入口方法，解析命令行参数并执行对应操作
   * @param argv 命令行参数数组
   * @return 执行结果退出码，0成功，-1失败
   * @throws Exception 执行过程中抛出的异常
   */
  public int run(String[] argv) throws Exception {
    int exitCode = -1;
    if (argv.length < 1) {
      displayUsage("");
      return exitCode;
    }    
    // 处理命令行参数
    String cmd = argv[0];
    String submitJobFile = null;
    String jobid = null;
    String taskid = null;
    String historyFileOrJobId = null;
    String historyOutFile = null;
    String historyOutFormat = HistoryViewer.HUMAN_FORMAT;
    String counterGroupName = null;
    String counterName = null;
    JobPriority jp = null;
    String taskType = null;
    String taskState = null;
    int fromEvent = 0;
    int nEvents = 0;
    int jpvalue = 0;
    String configOutFile = null;
    boolean getStatus = false;
    boolean getCounter = false;
    boolean killJob = false;
    boolean listEvents = false;
    boolean viewHistory = false;
    boolean viewAllHistory = false;
    boolean listJobs = false;
    boolean listAllJobs = false;
    boolean listActiveTrackers = false;
    boolean listBlacklistedTrackers = false;
    boolean displayTasks = false;
    boolean killTask = false;
    boolean failTask = false;
    boolean setJobPriority = false;
    boolean logs = false;
    boolean downloadConfig = false;

    // 解析-submit命令：提交作业
    if ("-submit".equals(cmd)) {
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      submitJobFile = argv[1];
    } else if ("-status".equals(cmd)) {
      // 解析-status命令：查询作业状态
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      getStatus = true;
    } else if("-counter".equals(cmd)) {
      // 解析-counter命令：获取作业计数器值
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      getCounter = true;
      jobid = argv[1];
      counterGroupName = argv[2];
      counterName = argv[3];
    } else if ("-kill".equals(cmd)) {
      // 解析-kill命令：杀死指定作业
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      killJob = true;
    } else if ("-set-priority".equals(cmd)) {
      // 解析-set-priority命令：修改作业优先级
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      try {
        jp = JobPriority.valueOf(argv[2]);
      } catch (IllegalArgumentException iae) {
        try {
          jpvalue = Integer.parseInt(argv[2]);
        } catch (NumberFormatException ne) {
          LOG.info("Error number format: ", ne);
          displayUsage(cmd);
          return exitCode;
        }
      }
      setJobPriority = true; 
    } else if ("-events".equals(cmd)) {
      // 解析-events命令：列出作业任务完成事件
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      fromEvent = Integer.parseInt(argv[2]);
      nEvents = Integer.parseInt(argv[3]);
      listEvents = true;
    } else if ("-history".equals(cmd)) {
      // 解析-history命令：查看作业历史
      viewHistory = true;
      if (argv.length < 2 || argv.length > 7) {
        displayUsage(cmd);
        return exitCode;
      }

      // 处理可选参数：[all] <jobHistoryFile|jobId> [-outfile <file>] [-format <human|json>]
      // "all"参数可选，若存在则放在最前面
      int index = 1;
      if ("all".equals(argv[index])) {
        index++;
        viewAllHistory = true;
        if (argv.length == 2) {
          displayUsage(cmd);
          return exitCode;
        }
      }
      // 获取历史文件路径或作业ID
      historyFileOrJobId = argv[index++];
      // 处理-outfile参数：指定输出文件
      if (argv.length > index + 1 && "-outfile".equals(argv[index])) {
        index++;
        historyOutFile = argv[index++];
      }
      // 处理-format参数：指定输出格式
      if (argv.length > index + 1 && "-format".equals(argv[index])) {
        index++;
        historyOutFormat = argv[index++];
      }
      // 检查是否有多余参数
      if (argv.length > index) {
        displayUsage(cmd);
        return exitCode;
      }
    } else if ("-list".equals(cmd)) {
      // 解析-list命令：列出作业
      if (argv.length != 1 && !(argv.length == 2 && "all".equals(argv[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && "all".equals(argv[1])) {
        listAllJobs = true;
      } else {
        listJobs = true;
      }
    } else if("-kill-task".equals(cmd)) {
      // 解析-kill-task命令：杀死指定任务尝试
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      killTask = true;
      taskid = argv[1];
    } else if("-fail-task".equals(cmd)) {
      // 解析-fail-task命令：将指定任务尝试标记为失败
      if (argv.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      failTask = true;
      taskid = argv[1];
    } else if ("-list-active-trackers".equals(cmd)) {
      // 解析-list-active-trackers命令：列出活跃的TaskTracker
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listActiveTrackers = true;
    } else if ("-list-blacklisted-trackers".equals(cmd)) {
      // 解析-list-blacklisted-trackers命令：列出黑名单中的TaskTracker
      if (argv.length != 1) {
        displayUsage(cmd);
        return exitCode;
      }
      listBlacklistedTrackers = true;
    } else if ("-list-attempt-ids".equals(cmd)) {
      // 解析-list-attempt-ids命令：列出指定类型和状态的任务尝试ID
      if (argv.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      taskType = argv[2];
      taskState = argv[3];
      displayTasks = true;
      // 验证任务类型合法性
      if (!taskTypes.contains(
          org.apache.hadoop.util.StringUtils.toUpperCase(taskType))) {
        System.out.println("Error: Invalid task-type: " + taskType);
        displayUsage(cmd);
        return exitCode;
      }
      // 验证任务状态合法性
      if (!taskStates.contains(
          org.apache.hadoop.util.StringUtils.toLowerCase(taskState))) {
        System.out.println("Error: Invalid task-state: " + taskState);
        displayUsage(cmd);
        return exitCode;
      }
    } else if ("-logs".equals(cmd)) {
      // 解析-logs命令：获取任务尝试日志
      if (argv.length == 2 || argv.length ==3) {
        logs = true;
        jobid = argv[1];
        if (argv.length == 3) {
          taskid = argv[2];
        }  else {
          taskid = null;
        }
      } else {
        displayUsage(cmd);
        return exitCode;
      }
    } else if ("-config".equals(cmd)) {
      // 解析-config命令：下载作业配置文件到本地
      downloadConfig = true;
      if (argv.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = argv[1];
      configOutFile = argv[2];
    } else {
      // 未知命令，显示帮助
      displayUsage(cmd);
      return exitCode;
    }

    // 初始化集群连接
    cluster = createCluster();
        
    // 根据解析结果执行对应操作
    try {
      if (submitJobFile != null) {
        // 提交作业
        Job job = Job.getInstance(new JobConf(submitJobFile));
        job.submit();
        System.out.println("Created job " + job.getJobID());
        exitCode = 0;
      } else if (getStatus) {
        // 查询作业状态和计数器
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          System.out.println();
          System.out.println(job);
          if (counters != null) {
            System.out.println(counters);
          } else {
            System.out.println("Counters not available. Job is retired.");
          }
          exitCode = 0;
        }
      } else if (getCounter) {
        // 获取指定计数器值
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          Counters counters = job.getCounters();
          if (counters == null) {
            System.out.println("Counters not available for retired job " + 
            jobid);
            exitCode = -1;
          } else {
            System.out.println(getCounter(counters,
              counterGroupName, counterName));
            exitCode = 0;
          }
        }
      } else if (killJob) {
        // 杀死指定作业
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          JobStatus jobStatus = job.getStatus();
          if (jobStatus.getState() == JobStatus.State.FAILED) {
            System.out.println("Could not mark the job " + jobid
                + " as killed, as it has already failed.");
            exitCode = -1;
          } else if (jobStatus.getState() == JobStatus.State.KILLED) {
            System.out
                .println("The job " + jobid + " has already been killed.");
            exitCode = -1;
          } else if (jobStatus.getState() == JobStatus.State.SUCCEEDED) {
            System.out.println("Could not kill the job " + jobid
                + ", as it has already succeeded.");
            exitCode = -1;
          } else {
            job.killJob();
            System.out.println("Killed job " + jobid);
            exitCode = 0;
          }
        }
      } else if (setJobPriority) {
        // 修改作业优先级
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          if (jp != null) {
            job.setPriority(jp);
          } else {
            job.setPriorityAsInteger(jpvalue);
          }
          System.out.println("Changed job priority.");
          exitCode = 0;
        } 
      } else if (viewHistory) {
        // 查看作业历史
        // 根据后缀判断是历史文件还是作业ID
        if (historyFileOrJobId.endsWith(".jhist")) {
          viewHistory(historyFileOrJobId, viewAllHistory, historyOutFile,
              historyOutFormat);
          exitCode = 0;
        } else {
          Job job = getJob(JobID.forName(historyFileOrJobId));
          if (job == null) {
            System.out.println("Could not find job " + jobid);
          } else {
            String historyUrl = job.getHistoryUrl();
            if (historyUrl == null || historyUrl.isEmpty()) {
              System.out.println("History file for job " + historyFileOrJobId +
                  " is currently unavailable.");
            } else {
              viewHistory(historyUrl, viewAllHistory, historyOutFile,
                  historyOutFormat);
              exitCode = 0;
            }
          }
        }
      } else if (listEvents) {
        // 列出指定范围的任务完成事件
        Job job = getJob(JobID.forName(jobid));
        if (job == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          listEvents(job, fromEvent, nEvents);
          exitCode = 0;
        }
      } else if (listJobs) {
        // 列出所有运行中的作业
        listJobs(cluster);
        exitCode = 0;
      } else if (listAllJobs) {
        // 列出所有已提交作业
        listAllJobs(cluster);
        exitCode = 0;
      } else if (listActiveTrackers) {