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

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * 为人可读格式打印MapReduce作业历史的实现类，供HistoryViewer使用
 * 将解析后的作业历史转换为适合人类阅读的格式化文本输出
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class HumanReadableHistoryViewerPrinter implements HistoryViewerPrinter {

  private JobHistoryParser.JobInfo job;
  private final FastDateFormat dateFormat;
  private boolean printAll;
  private String scheme;

  /**
   * 构造HumanReadableHistoryViewerPrinter，使用默认时区
   * @param job 解析后的作业信息对象
   * @param printAll 是否打印所有任务（包括成功的任务和所有尝试）
   * @param scheme 日志URL的协议类型
   */
  HumanReadableHistoryViewerPrinter(JobHistoryParser.JobInfo job,
                                    boolean printAll, String scheme) {
    this(job, printAll, scheme, TimeZone.getDefault());
  }

  /**
   * 构造HumanReadableHistoryViewerPrinter，指定时区
   * @param job 解析后的作业信息对象
   * @param printAll 是否打印所有任务（包括成功的任务和所有尝试）
   * @param scheme 日志URL的协议类型
   * @param tz 日期格式化使用的时区
   */
  HumanReadableHistoryViewerPrinter(JobHistoryParser.JobInfo job,
                                    boolean printAll, String scheme,
                                    TimeZone tz) {
    this.job = job;
    this.printAll = printAll;
    this.scheme = scheme;
    this.dateFormat = FastDateFormat.getInstance("d-MMM-yyyy HH:mm:ss", tz);
  }

  /**
   * 将作业历史以人类可读格式打印到指定输出流
   * @param ps 输出流
   * @throws IOException 打印过程中IO异常
   */
  @Override
  public void print(PrintStream ps) throws IOException {
    // 打印作业基本信息
    printJobDetails(ps);
    // 打印各类任务汇总统计
    printTaskSummary(ps);
    // 打印作业性能分析
    printJobAnalysis(ps);
    // 打印不同类型不同状态的失败/被杀死任务列表
    printTasks(ps, TaskType.JOB_SETUP, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.JOB_SETUP, TaskStatus.State.KILLED.toString());
    printTasks(ps, TaskType.MAP, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.MAP, TaskStatus.State.KILLED.toString());
    printTasks(ps, TaskType.REDUCE, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.REDUCE, TaskStatus.State.KILLED.toString());
    printTasks(ps, TaskType.JOB_CLEANUP, TaskStatus.State.FAILED.toString());
    printTasks(ps, TaskType.JOB_CLEANUP,
        JobStatus.getJobRunState(JobStatus.KILLED));
    // 如果需要打印全部内容，打印成功任务和所有任务尝试
    if (printAll) {
      printTasks(ps, TaskType.JOB_SETUP, TaskStatus.State.SUCCEEDED.toString());
      printTasks(ps, TaskType.MAP, TaskStatus.State.SUCCEEDED.toString());
      printTasks(ps, TaskType.REDUCE, TaskStatus.State.SUCCEEDED.toString());
      printTasks(ps, TaskType.JOB_CLEANUP,
          TaskStatus.State.SUCCEEDED.toString());
      printAllTaskAttempts(ps, TaskType.JOB_SETUP);
      printAllTaskAttempts(ps, TaskType.MAP);
      printAllTaskAttempts(ps, TaskType.REDUCE);
      printAllTaskAttempts(ps, TaskType.JOB_CLEANUP);
    }

    // 按节点统计失败的任务尝试
    HistoryViewer.FilteredJob filter = new HistoryViewer.FilteredJob(job,
        TaskStatus.State.FAILED.toString());
    printFailedAttempts(ps, filter);

    filter = new HistoryViewer.FilteredJob(job,
        TaskStatus.State.KILLED.toString());
    printFailedAttempts(ps, filter);
  }

  /**
   * 打印作业基本详细信息
   * @param ps 输出流
   */
  private void printJobDetails(PrintStream ps) {
    StringBuilder jobDetails = new StringBuilder();
    jobDetails.append("\nHadoop job: ").append(job.getJobId());
    jobDetails.append("\n=====================================");
    jobDetails.append("\nUser: ").append(job.getUsername());
    jobDetails.append("\nJobName: ").append(job.getJobname());
    jobDetails.append("\nJobConf: ").append(job.getJobConfPath());
    jobDetails.append("\nSubmitted At: ").append(StringUtils.
        getFormattedTimeWithDiff(dateFormat,
            job.getSubmitTime(), 0));
    jobDetails.append("\nLaunched At: ").append(StringUtils.
        getFormattedTimeWithDiff(dateFormat,
            job.getLaunchTime(),
            job.getSubmitTime()));
    jobDetails.append("\nFinished At: ").append(StringUtils.
        getFormattedTimeWithDiff(dateFormat,
            job.getFinishTime(),
            job.getLaunchTime()));
    jobDetails.append("\nStatus: ").append(((job.getJobStatus() == null) ?
        "Incomplete" :job.getJobStatus()));
    // 打印作业计数器信息
    printJobCounters(jobDetails, job.getTotalCounters(), job.getMapCounters(),
        job.getReduceCounters());
    jobDetails.append("\n");
    jobDetails.append("\n=====================================");
    ps.println(jobDetails);
  }

  /**
   * 格式化打印作业计数器，分别展示Map端、Reduce端和总计数器值
   * @param buff 字符串拼接缓冲区
   * @param totalCounters 总计数器
   * @param mapCounters Map端计数器
   * @param reduceCounters Reduce端计数器
   */
  private void printJobCounters(StringBuilder buff, Counters totalCounters,
                                Counters mapCounters, Counters reduceCounters) {
    // 被杀死的作业可能没有计数器
    if (totalCounters != null) {
      buff.append("\nCounters: \n\n");
      buff.append(String.format("|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s|",
          "Group Name",
          "Counter name",
          "Map Value",
          "Reduce Value",
          "Total Value"));
      buff.append("\n------------------------------------------" +
          "---------------------------------------------");
      // 遍历所有计数器组
      for (CounterGroup counterGroup : totalCounters) {
        String groupName = counterGroup.getName();
        CounterGroup totalGroup = totalCounters.getGroup(groupName);
        CounterGroup mapGroup = mapCounters.getGroup(groupName);
        CounterGroup reduceGroup = reduceCounters.getGroup(groupName);

        Format decimal = new DecimalFormat();
        Iterator<Counter> ctrItr =
            totalGroup.iterator();
        // 遍历计数器组内每个计数器
        while (ctrItr.hasNext()) {
          org.apache.hadoop.mapreduce.Counter counter = ctrItr.next();
          String name = counter.getName();
          String mapValue =
              decimal.format(mapGroup.findCounter(name).getValue());
          String reduceValue =
              decimal.format(reduceGroup.findCounter(name).getValue());
          String totalValue =
              decimal.format(counter.getValue());

          buff.append(
              String.format("%n|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s",
                  totalGroup.getDisplayName(),
                  counter.getDisplayName(),
                  mapValue, reduceValue, totalValue));
        }
      }
    }
  }

  /**
   * 打印指定类型所有任务尝试的详细信息
   * @param ps 输出流
   * @param taskType 任务类型
   */
  private void printAllTaskAttempts(PrintStream ps, TaskType taskType) {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
    StringBuilder taskList = new StringBuilder();
    taskList.append("\n").append(taskType);
    taskList.append(" task list for ").append(job.getJobId());
    taskList.append("\nTaskId\t\tStartTime");
    // Reduce任务额外打印Shuffle和排序完成时间
    if (TaskType.REDUCE.equals(taskType)) {
      taskList.append("\tShuffleFinished\tSortFinished");
    }
    taskList.append("\tFinishTime\tHostName\tError\tTaskLogs");
    taskList.append("\n====================================================");
    ps.println(taskList.toString());
    // 遍历所有任务
    for (JobHistoryParser.TaskInfo task : tasks.values()) {
      // 遍历任务的所有尝试
      for (JobHistoryParser.TaskAttemptInfo attempt :
          task.getAllTaskAttempts().values()) {
        if (taskType.equals(task.getTaskType())){
          taskList.setLength(0);
          taskList.append(attempt.getAttemptId()).append("\t");
          taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
              attempt.getStartTime(), 0)).append("\t");
          if (TaskType.REDUCE.equals(taskType)) {
            taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                attempt.getShuffleFinishTime(),
                attempt.getStartTime()));
            taskList.append("\t");
            taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
                attempt.getSortFinishTime(),
                attempt.getShuffleFinishTime()));
          }
          taskList.append(StringUtils.getFormattedTimeWithDiff(dateFormat,
              attempt.getFinishTime(),
              attempt.getStartTime()));
          taskList.append("\t");
          taskList.append(attempt.getHostname()).append("\t");
          taskList.append(attempt.getError());
          // 拼接任务日志链接
          String taskLogsUrl = HistoryViewer.getTaskLogsUrl(scheme, attempt);
          taskList.append(taskLogsUrl != null ? taskLogsUrl : "n/a");
          ps.println(taskList);
        }
      }
    }
  }

  /**
   * 按任务类型打印任务统计汇总信息
   * @param ps 输出流
   */
  private void printTaskSummary(PrintStream ps) {
    HistoryViewer.SummarizedJob ts = new HistoryViewer.SummarizedJob(job);
    StringBuilder taskSummary = new StringBuilder();
    taskSummary.append("\nTask Summary");
    taskSummary.append("\n============================");
    taskSummary.append("\nKind\tTotal\t");
    taskSummary.append("Successful\tFailed\tKilled\tStartTime\tFinishTime");
    taskSummary.append("\n");
    // 打印JOB_SETUP任务统计
    taskSummary.append("\nSetup\t").append(ts.totalSetups);
    taskSummary.append("\t").append(ts.numFinishedSetups);
    taskSummary.append("\t\t").append(ts.numFailedSetups);
    taskSummary.append("\t").append(ts.numKilledSetups);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.setupStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.setupFinished, ts.setupStarted));
    // 打印MAP任务统计
    taskSummary.append("\nMap\t").append(ts.totalMaps);
    taskSummary.append("\t").append(job.getSucceededMaps());
    taskSummary.append("\t\t").append(ts.numFailedMaps);
    taskSummary.append("\t").append(ts.numKilledMaps);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.mapStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.mapFinished, ts.mapStarted));
    // 打印REDUCE任务统计
    taskSummary.append("\nReduce\t").append(ts.totalReduces);
    taskSummary.append("\t").append(job.getSucceededReduces());
    taskSummary.append("\t\t").append(ts.numFailedReduces);
    taskSummary.append("\t").append(ts.numKilledReduces);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.reduceStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.reduceFinished, ts.reduceStarted));
    // 打印JOB_CLEANUP任务统计
    taskSummary.append("\nCleanup\t").append(ts.totalCleanups);
    taskSummary.append("\t").append(ts.numFinishedCleanups);
    taskSummary.append("\t\t").append(ts.numFailedCleanups);
    taskSummary.append("\t").append(ts.numKilledCleanups);
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.cleanupStarted, 0));
    taskSummary.append("\t").append(StringUtils.getFormattedTimeWithDiff(
        dateFormat, ts.cleanupFinished,
        ts.cleanupStarted));
    taskSummary.append("\n============================\n");
    ps.println(taskSummary);
  }

  /**
   * 打印已完成作业的性能分析信息
   * @param ps 输出流
   */
  private void printJobAnalysis(PrintStream ps) {
    // 仅对成功完成的作业做分析
    if (job.getJobStatus().equals(
        JobStatus.getJobRunState(JobStatus.SUCCEEDED))) {
      HistoryViewer.AnalyzedJob avg = new HistoryViewer.AnalyzedJob(job);

      ps.println("\nAnalysis");
      ps.println("=========");
      // 分析Map任务运行时间
      printAnalysis(ps, avg.getMapTasks(), cMap, "map", avg.getAvgMapTime(),
          10);
      // 输出最后完成的Map任务信息
      printLast(ps, avg.getMapTasks(), "map", cFinishMapRed);

      // 如果存在Reduce任务，分析Shuffle和Reduce阶段
      if (avg.getReduceTasks().length > 0) {
        printAnalysis(ps, avg.getReduceTasks(), cShuffle, "shuffle",
            avg.getAvgShuffleTime(), 10);
        printLast(ps, avg.getReduceTasks(), "shuffle", cFinishShuffle);

        printAnalysis(ps, avg.getReduceTasks(), cReduce, "reduce",
            avg.getAvgReduceTime(), 10);
        printLast(ps, avg.getReduceTasks(), "reduce", cFinishMapRed);
      }
      ps.println("=========");
    } else {
      ps.println("No Analysis available as job did not finish");
    }
  }

  /**
   * 打印任务阶段性能分析，展示最快任务、平均时间和最慢的N个任务
   * @param ps 输出流
   * @param tasks 任务尝试信息数组
   * @param cmp 排序比较器
   * @param taskType 任务阶段类型（map/shuffle/reduce）
   * @param avg 该阶段平均耗时
   * @param showTasks 需要展示的最慢任务数量
   */
  protected void printAnalysis(PrintStream ps,
      JobHistoryParser.TaskAttemptInfo[] tasks,
      Comparator<JobHistoryParser.TaskAttemptInfo> cmp,
      String taskType, long avg, int showTasks) {
    Arrays.sort(tasks, cmp);
    // 排序后最后一个就是最快的任务
    JobHistoryParser.TaskAttemptInfo min = tasks[tasks.length-1];
    StringBuilder details = new StringBuilder();
    details.append("\nTime taken by best performing ");
    details.append(taskType).append(" task ");
    details.append(min.getAttemptId().getTaskID().toString()).append(": ");
    // 根据阶段类型计算耗时
    if ("map".equals(taskType)) {
      details.append(StringUtils.formatTimeDiff(
          min.getFinishTime(),
          min.getStartTime()));
    } else if ("shuffle".equals(taskType)) {
      details.append(StringUtils.formatTimeDiff(
          min.getShuffleFinishTime(),
          min.getStartTime()));
    } else {
      details.append(StringUtils.formatTimeDiff(