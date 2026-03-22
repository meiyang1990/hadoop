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
package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.util.HostUtil;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * 文件说明：MapReduce作业历史查看工具，负责解析并展示作业历史文件，支持人类可读格式和JSON两种输出格式
 * 核心功能：解析已完成作业的历史文件，生成作业、任务、任务尝试的汇总统计信息，并通过指定输出器打印结果
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HistoryViewer {
  private FileSystem fs;
  private JobInfo job;
  private HistoryViewerPrinter jhvp;
  public static final String HUMAN_FORMAT = "human";
  public static final String JSON_FORMAT = "json";

  /**
   * 构造HistoryViewer对象，默认使用人类可读格式输出
   * @param historyFile 历史文件的完整路径
   * @param conf Hadoop配置对象
   * @param printAll 是否打印所有任务状态，false仅打印失败/被杀死的任务
   * @throws IOException 解析历史文件失败时抛出
   */
  public HistoryViewer(String historyFile, Configuration conf,
                       boolean printAll) throws IOException {
    this(historyFile, conf, printAll, HUMAN_FORMAT);
  }

  /**
   * 构造HistoryViewer对象，支持指定输出格式
   * @param historyFile 历史文件的完整路径
   * @param conf Hadoop配置对象
   * @param printAll 是否打印所有任务状态，false仅打印失败/被杀死的任务
   * @param format 输出格式，支持human/json两种
   * @throws IOException 解析历史文件失败时抛出
   */
  public HistoryViewer(String historyFile, Configuration conf, boolean printAll,
                       String format) throws IOException {
    String errorMsg = "Unable to initialize History Viewer";
    try {
      // 构造作业历史文件路径对象
      Path jobFile = new Path(historyFile);
      // 获取文件系统实例
      fs = jobFile.getFileSystem(conf);
      // 拆分文件名获取作业信息
      String[] jobDetails =
        jobFile.getName().split("_");
      // 文件名格式不合法，忽略该文件
      if (jobDetails.length < 2) {
        // NOT a valid name
        System.err.println("Ignore unrecognized file: " + jobFile.getName());
        throw new IOException(errorMsg);
      }
      // 创建作业历史解析器
      JobHistoryParser parser = new JobHistoryParser(fs, jobFile);
      // 解析历史文件获取作业信息
      job = parser.parse();
      // 获取Web服务协议前缀(http/https)
      String scheme = WebAppUtils.getHttpSchemePrefix(fs.getConf());
      // 根据格式选择对应输出器
      if (HUMAN_FORMAT.equalsIgnoreCase(format)) {
        jhvp = new HumanReadableHistoryViewerPrinter(job, printAll, scheme);
      } else if (JSON_FORMAT.equalsIgnoreCase(format)) {
        jhvp = new JSONHistoryViewerPrinter(job, printAll, scheme);
      } else {
        System.err.println("Invalid format specified: " + format);
        throw new IllegalArgumentException(errorMsg);
      }
    } catch(IOException e) {
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * 将作业历史摘要信息打印到标准输出
   * @throws IOException 打印过程出错时抛出
   */
  public void print() throws IOException {
    print(System.out);
  }

  /**
   * 将作业历史摘要信息打印到指定输出流
   * @param ps 目标打印流
   * @throws IOException 打印过程出错时抛出
   */
  public void print(PrintStream ps) throws IOException {
    jhvp.print(ps);
  }
  
  /**
   * 生成指定任务尝试的日志访问URL
   * 
   * @param scheme Web协议前缀(http/https)
   * @param attempt 任务尝试信息对象
   * @return 任务日志访问URL，必要信息缺失时返回null
   */
  public static String getTaskLogsUrl(String scheme,
      JobHistoryParser.TaskAttemptInfo attempt) {
    // 必要信息缺失，返回null
    if (attempt.getHttpPort() == -1
        || attempt.getTrackerName().equals("")
        || attempt.getAttemptId() == null) {
      return null;
    }
  
    // 从TrackerName中提取主机名
    String taskTrackerName =
      HostUtil.convertTrackerNameToHostName(
        attempt.getTrackerName());
    // 拼接生成任务日志URL并返回
    return HostUtil.getTaskLogUrl(scheme, taskTrackerName,
        Integer.toString(attempt.getHttpPort()),
        attempt.getAttemptId().toString());
  }

  /**
   * 作业汇总统计类，存储作业各类型任务的统计信息，供HistoryViewer和作业历史WebUI使用
   */
  public static class SummarizedJob {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks; 
     int totalMaps = 0; 
     int totalReduces = 0; 
     int totalCleanups = 0;
     int totalSetups = 0;
     int numFailedMaps = 0; 
     int numKilledMaps = 0;
     int numFailedReduces = 0; 
     int numKilledReduces = 0;
     int numFinishedCleanups = 0;
     int numFailedCleanups = 0;
     int numKilledCleanups = 0;
     int numFinishedSetups = 0;
     int numFailedSetups = 0;
     int numKilledSetups = 0;
     long mapStarted = 0; 
     long mapFinished = 0; 
     long reduceStarted = 0; 
     long reduceFinished = 0; 
     long cleanupStarted = 0;
     long cleanupFinished = 0;
     long setupStarted = 0;
     long setupFinished = 0;
     
     /** Get total maps */
     public int getTotalMaps() { return totalMaps; } 
     /** Get total reduces */
     public int getTotalReduces() { return totalReduces; } 
     /** Get number of clean up tasks */ 
     public int getTotalCleanups() { return totalCleanups; }
     /** Get number of set up tasks */
     public int getTotalSetups() { return totalSetups; }
     /** Get number of failed maps */
     public int getNumFailedMaps() { return numFailedMaps; }
     /** Get number of killed maps */
     public int getNumKilledMaps() { return numKilledMaps; }
     /** Get number of failed reduces */
     public int getNumFailedReduces() { return numFailedReduces; } 
     /** Get number of killed reduces */
     public int getNumKilledReduces() { return numKilledReduces; }
     /** Get number of cleanup tasks that finished */
     public int getNumFinishedCleanups() { return numFinishedCleanups; }
     /** Get number of failed cleanup tasks */
     public int getNumFailedCleanups() { return numFailedCleanups; }
     /** Get number of killed cleanup tasks */
     public int getNumKilledCleanups() { return numKilledCleanups; }
     /** Get number of finished set up tasks */
     public int getNumFinishedSetups() { return numFinishedSetups; }
     /** Get number of failed set up tasks */
     public int getNumFailedSetups() { return numFailedSetups; }
     /** Get number of killed set up tasks */
     public int getNumKilledSetups() { return numKilledSetups; }
     /** Get number of maps that were started */
     public long getMapStarted() { return mapStarted; } 
     /** Get number of maps that finished */
     public long getMapFinished() { return mapFinished; } 
     /** Get number of Reducers that were started */
     public long getReduceStarted() { return reduceStarted; } 
     /** Get number of reducers that finished */
     public long getReduceFinished() { return reduceFinished; } 
     /** Get number of cleanup tasks started */ 
     public long getCleanupStarted() { return cleanupStarted; }
     /** Get number of cleanup tasks that finished */
     public long getCleanupFinished() { return cleanupFinished; }
     /** Get number of setup tasks that started */
     public long getSetupStarted() { return setupStarted; }
     /** Get number of setup tasks that finished */
     public long getSetupFinished() { return setupFinished; }

     /**
      * 根据解析后的作业信息生成汇总统计
      * @param job 解析完成的作业信息对象
      */
    public SummarizedJob(JobInfo job) {
      tasks = job.getAllTasks();

      // 遍历所有任务
      for (JobHistoryParser.TaskInfo task : tasks.values()) {
        Map<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts = 
          task.getAllTaskAttempts();
        // 遍历所有任务尝试
        for (JobHistoryParser.TaskAttemptInfo attempt : attempts.values()) {
          long startTime = attempt.getStartTime(); 
          long finishTime = attempt.getFinishTime();
          // 根据任务类型分类统计
          if (attempt.getTaskType().equals(TaskType.MAP)) {
            // 记录最早map开始时间
            if (mapStarted== 0 || mapStarted > startTime) {
              mapStarted = startTime; 
            }
            // 记录最晚map结束时间
            if (mapFinished < finishTime) {
              mapFinished = finishTime; 
            }
            totalMaps++; 
            // 统计失败和被杀死的map数量
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedMaps++; 
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledMaps++;
            }
          } else if (attempt.getTaskType().equals(TaskType.REDUCE)) {
            // 记录最早reduce开始时间
            if (reduceStarted==0||reduceStarted > startTime) {
              reduceStarted = startTime; 
            }
            // 记录最晚reduce结束时间
            if (reduceFinished < finishTime) {
              reduceFinished = finishTime; 
            }
            totalReduces++; 
            // 统计失败和被杀死的reduce数量
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedReduces++; 
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledReduces++;
            }
          } else if (attempt.getTaskType().equals(TaskType.JOB_CLEANUP)) {
            // 记录最早清理任务开始时间
            if (cleanupStarted==0||cleanupStarted > startTime) {
              cleanupStarted = startTime; 
            }
            // 记录最晚清理任务结束时间
            if (cleanupFinished < finishTime) {
              cleanupFinished = finishTime; 
            }
            totalCleanups++; 
            // 按状态统计清理任务数量
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.SUCCEEDED.toString())) {
              numFinishedCleanups++; 
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedCleanups++;
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledCleanups++;
            }
          } else if (attempt.getTaskType().equals(TaskType.JOB_SETUP)) {
            // 记录最早初始化任务开始时间
            if (setupStarted==0||setupStarted > startTime) {
              setupStarted = startTime; 
            }
            // 记录最晚初始化任务结束时间
            if (setupFinished < finishTime) {
              setupFinished = finishTime; 
            }
            totalSetups++; 
            // 按状态统计初始化任务数量
            if (attempt.getTaskStatus().equals
                (TaskStatus.State.SUCCEEDED.toString())) {
              numFinishedSetups++;
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.FAILED.toString())) {
              numFailedSetups++;
            } else if (attempt.getTaskStatus().equals
                (TaskStatus.State.KILLED.toString())) {
              numKilledSetups++;
            }
          }
        }
      }
    }
  }

  /**
   * 作业分析类，存储成功任务的平均运行时间统计和任务列表，供HistoryViewer和作业历史WebUI使用
   */
  public static class AnalyzedJob {
    private long avgMapTime;
    private long avgReduceTime;
    private long avgShuffleTime;
    
    private JobHistoryParser.TaskAttemptInfo [] mapTasks;
    private JobHistoryParser.TaskAttemptInfo [] reduceTasks;

    /** Get the average map time */
    public long getAvgMapTime() { return avgMapTime; }
    /** Get the average reduce time */
    public long getAvgReduceTime() { return avgReduceTime; }
    /** Get the average shuffle time */
    public long getAvgShuffleTime() { return avgShuffleTime; }
    /** Get the map tasks list */
    public JobHistoryParser.TaskAttemptInfo [] getMapTasks() { 
      return mapTasks;
    }
    /** Get the reduce tasks list */
    public JobHistoryParser.TaskAttemptInfo [] getReduceTasks() { 
      return reduceTasks;
    }
    /**
     * 根据解析后的作业信息生成分析统计
     * @param job 解析完成的作业信息对象
     */
    public AnalyzedJob (JobInfo job) {
      Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
      int succeededMaps = (int) job.getSucceededMaps();
      int succeededReduces = (int) job.getSucceededReduces();
      // 初始化成功map数组
      mapTasks = 
        new JobHistoryParser.TaskAttemptInfo[succeededMaps];
      // 初始化成功reduce数组
      reduceTasks = 
        new JobHistoryParser.TaskAttemptInfo[succeededReduces];
      int mapIndex = 0 , reduceIndex=0; 
      avgMapTime = 0;
      avgReduceTime = 0;
      avgShuffleTime = 0;

      // 遍历所有任务
      for (JobHistoryParser.TaskInfo task : tasks.values()) {
        Map<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts =
          task.getAllTaskAttempts();
        // 遍历所有任务尝试
        for (JobHistoryParser.TaskAttemptInfo attempt : attempts.values()) {
          // 只统计成功完成的任务尝试
          if (attempt.getTaskStatus().
              equals(TaskStatus.State.SUCCEEDED.toString())) {
            // 计算任务运行时长
            long avgFinishTime = (attempt.getFinishTime() -
                attempt.getStartTime());
            if (attempt.getTaskType().equals(TaskType.MAP)) {
              // 添加到成功map列表，累加总运行时间
              mapTasks[mapIndex++] = attempt; 
              avgMapTime += avgFinishTime;
            } else if (attempt.getTaskType().equals(TaskType.REDUCE)) {
              // 添加到成功reduce列表，累加shuffle时间和reduce运行时间
              reduceTasks[reduceIndex++] = attempt;
              avgShuffleTime += (attempt.getShuffleFinishTime() - 
                  attempt.getStartTime());
              avgReduceTime += (attempt.getFinishTime() -
                  attempt.getShuffleFinishTime());
            }
            // 每个任务只取第一个成功尝试统计，跳出循环
            break;
          }
        }
      }
      // 计算平均运行时间，防止除以0
      if (succeededMaps > 0) {
        avgMapTime /= succeededMaps;
      }
      if (succeededReduces > 0) {
        avgReduceTime /= succeededReduces;
        avgShuffleTime /= succeededReduces;
      }
    }
  }

  /**
   * 按任务状态过滤作业，统计各节点上符合过滤条件的任务，供历史分析使用
   */
  public static class FilteredJob {
    
    private Map<String, Set<TaskID>> badNodesToFilteredTasks =
      new HashMap<String, Set<TaskID>>();
    
    private String filter;
    
    /** Get the map of the filtered tasks */
    public Map<String, Set<TaskID>> getFilteredMap() {
      return badNodesToFilteredTasks;
    }