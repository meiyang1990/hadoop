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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

/**
 * 供HistoryViewer使用的JSON格式作业历史输出器，将作业历史以机器可读的JSON格式输出
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class JSONHistoryViewerPrinter implements HistoryViewerPrinter {

  private JobHistoryParser.JobInfo job;
  private boolean printAll;
  private String scheme;
  private JSONObject json;

  /**
   * 构造JSON格式历史输出器
   * @param job 解析完成的作业信息对象
   * @param printAll 是否输出所有任务尝试信息
   * @param scheme 访问日志的URL协议
   */
  JSONHistoryViewerPrinter(JobHistoryParser.JobInfo job, boolean printAll,
                           String scheme) {
    this.job = job;
    this.printAll = printAll;
    this.scheme = scheme;
  }

  /**
   * 将作业历史以JSON格式输出到指定PrintStream
   * @param ps 输出目标PrintStream
   * @throws IOException 输出过程中发生IO异常时抛出
   */
  @Override
  public void print(PrintStream ps) throws IOException {
    json = new JSONObject();

    Writer writer = null;
    try {
      // 输出作业基本信息
      printJobDetails();
      // 输出各类任务统计汇总
      printTaskSummary();
      // 输出失败/被杀死的任务详情
      printTasks();

      writer = new OutputStreamWriter(ps, StandardCharsets.UTF_8);
      json.write(writer);
      writer.flush();
    } catch (JSONException je) {
      throw new IOException("Failure parsing JSON document: " + je.getMessage(),
          je);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  /**
   * 将作业基本信息填充到JSON对象中
   * @throws JSONException JSON构造异常时抛出
   */
  private void printJobDetails() throws JSONException {
    json.put("hadoopJob", job.getJobId().toString());
    json.put("user", job.getUsername());
    json.put("jobName", job.getJobname());
    json.put("jobConf", job.getJobConfPath());
    json.put("submittedAt", job.getSubmitTime());
    json.put("launchedAt", job.getLaunchTime());
    json.put("finishedAt", job.getFinishTime());
    json.put("status", ((job.getJobStatus() == null) ?
        "Incomplete" :job.getJobStatus()));
    printJobCounters(job.getTotalCounters(), job.getMapCounters(),
        job.getReduceCounters());
  }

  /**
   * 将作业全局计数器信息填充到JSON对象中，分别统计Map、Reduce和总计
   * @param totalCounters 作业全局计数器
   * @param mapCounters Map阶段总计数器
   * @param reduceCounters Reduce阶段总计数器
   * @throws JSONException JSON构造异常时抛出
   */
  private void printJobCounters(Counters totalCounters, Counters mapCounters,
                                Counters reduceCounters) throws JSONException {
    // Killed jobs might not have counters
    if (totalCounters != null) {
      JSONObject jGroups = new JSONObject();
      for (CounterGroup counterGroup : totalCounters) {
        String groupName = counterGroup.getName();
        CounterGroup totalGroup = totalCounters.getGroup(groupName);
        CounterGroup mapGroup = mapCounters.getGroup(groupName);
        CounterGroup reduceGroup = reduceCounters.getGroup(groupName);

        Iterator<Counter> ctrItr =
            totalGroup.iterator();
        JSONArray jGroup = new JSONArray();
        while (ctrItr.hasNext()) {
          JSONObject jCounter = new JSONObject();
          org.apache.hadoop.mapreduce.Counter counter = ctrItr.next();
          String name = counter.getName();
          long mapValue = mapGroup.findCounter(name).getValue();
          long reduceValue = reduceGroup.findCounter(name).getValue();
          long totalValue = counter.getValue();

          jCounter.put("counterName", name);
          jCounter.put("mapValue", mapValue);
          jCounter.put("reduceValue", reduceValue);
          jCounter.put("totalValue", totalValue);
          jGroup.put(jCounter);
        }
        jGroups.put(fixGroupNameForShuffleErrors(totalGroup.getName()), jGroup);
      }
      json.put("counters", jGroups);
    }
  }

  /**
   * 将不同类型任务的统计汇总信息填充到JSON对象中
   * @throws JSONException JSON构造异常时抛出
   */
  private void printTaskSummary() throws JSONException {
    HistoryViewer.SummarizedJob ts = new HistoryViewer.SummarizedJob(job);
    JSONObject jSums = new JSONObject();
    JSONObject jSumSetup = new JSONObject();
    jSumSetup.put("total", ts.totalSetups);
    jSumSetup.put("successful", ts.numFinishedSetups);
    jSumSetup.put("failed", ts.numFailedSetups);
    jSumSetup.put("killed", ts.numKilledSetups);
    jSumSetup.put("startTime", ts.setupStarted);
    jSumSetup.put("finishTime", ts.setupFinished);
    jSums.put("setup", jSumSetup);
    JSONObject jSumMap = new JSONObject();
    jSumMap.put("total", ts.totalMaps);
    jSumMap.put("successful", job.getSucceededMaps());
    jSumMap.put("failed", ts.numFailedMaps);
    jSumMap.put("killed", ts.numKilledMaps);
    jSumMap.put("startTime", ts.mapStarted);
    jSumMap.put("finishTime", ts.mapFinished);
    jSums.put("map", jSumMap);
    JSONObject jSumReduce = new JSONObject();
    jSumReduce.put("total", ts.totalReduces);
    jSumReduce.put("successful", job.getSucceededReduces());
    jSumReduce.put("failed", ts.numFailedReduces);
    jSumReduce.put("killed", ts.numKilledReduces);
    jSumReduce.put("startTime", ts.reduceStarted);
    jSumReduce.put("finishTime", ts.reduceFinished);
    jSums.put("reduce", jSumReduce);
    JSONObject jSumCleanup = new JSONObject();
    jSumCleanup.put("total", ts.totalCleanups);
    jSumCleanup.put("successful", ts.numFinishedCleanups);
    jSumCleanup.put("failed", ts.numFailedCleanups);
    jSumCleanup.put("killed", ts.numKilledCleanups);
    jSumCleanup.put("startTime", ts.cleanupStarted);
    jSumCleanup.put("finishTime", ts.cleanupFinished);
    jSums.put("cleanup", jSumCleanup);
    json.put("taskSummary", jSums);
  }

  /**
   * 将失败/被杀死的任务详情填充到JSON对象中，printAll开启时会输出所有成功任务和任务尝试信息
   * @throws JSONException JSON构造异常时抛出
   */
  private void printTasks() throws JSONException {
    Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();
    JSONArray jTasks = new JSONArray();
    for (JobHistoryParser.TaskInfo task : tasks.values()) {
      // 跳过任务清理任务，只输出需要展示的任务
      if (!task.getTaskType().equals(TaskType.TASK_CLEANUP) &&
          ((printAll && task.getTaskStatus().equals(
              TaskStatus.State.SUCCEEDED.toString()))
          || task.getTaskStatus().equals(TaskStatus.State.KILLED.toString())
          || task.getTaskStatus().equals(TaskStatus.State.FAILED.toString()))) {
        JSONObject jTask = new JSONObject();
        jTask.put("taskId", task.getTaskId().toString());
        jTask.put("type", task.getTaskType().toString());
        jTask.put("status", task.getTaskStatus());
        jTask.put("startTime", task.getStartTime());
        jTask.put("finishTime", task.getFinishTime());
        // 有错误信息则添加错误信息
        if (!task.getError().isEmpty()) {
          jTask.put("error", task.getError());
        }
        // Map任务添加输入块位置信息
        if (task.getTaskType().equals(TaskType.MAP)) {
          jTask.put("inputSplits", task.getSplitLocations());
        }
        // printAll开启时输出任务计数器和所有任务尝试信息
        if (printAll) {
          printTaskCounters(jTask, task.getCounters());
          JSONObject jAtt = new JSONObject();
          for (JobHistoryParser.TaskAttemptInfo attempt :
              task.getAllTaskAttempts().values()) {
            jAtt.put("attemptId", attempt.getAttemptId());
            jAtt.put("startTime", attempt.getStartTime());
            // Reduce任务添加shuffle和排序完成时间
            if (task.getTaskType().equals(TaskType.REDUCE)) {
              jAtt.put("shuffleFinished", attempt.getShuffleFinishTime());
              jAtt.put("sortFinished", attempt.getSortFinishTime());
            }
            jAtt.put("finishTime", attempt.getFinishTime());
            jAtt.put("hostName", attempt.getHostname());
            // 有错误信息则添加错误信息
            if (!attempt.getError().isEmpty()) {
              jAtt.put("error", task.getError());
            }
            // 生成任务日志访问URL并添加
            String taskLogsUrl = HistoryViewer.getTaskLogsUrl(scheme, attempt);
            if (taskLogsUrl != null) {
              jAtt.put("taskLogs", taskLogsUrl);
            }
          }
          jTask.put("attempts", jAtt);
        }
        jTasks.put(jTask);
      }
      json.put("tasks", jTasks);
    }
  }

  /**
   * 将单个任务的计数器信息填充到任务JSON对象中
   * @param jTask 任务JSON对象
   * @param taskCounters 任务计数器
   * @throws JSONException JSON构造异常时抛出
   */
  private void printTaskCounters(JSONObject jTask, Counters taskCounters)
      throws JSONException {
    // Killed tasks might not have counters
    if (taskCounters != null) {
      JSONObject jGroups = new JSONObject();
      for (String groupName : taskCounters.getGroupNames()) {
        CounterGroup group = taskCounters.getGroup(groupName);

        Iterator<Counter> ctrItr = group.iterator();
        JSONArray jGroup = new JSONArray();
        while (ctrItr.hasNext()) {
          JSONObject jCounter = new JSONObject();
          org.apache.hadoop.mapreduce.Counter counter = ctrItr.next();

          jCounter.put("counterName", counter.getName());
          jCounter.put("value", counter.getValue());
          jGroup.put(jCounter);
        }
        jGroups.put(fixGroupNameForShuffleErrors(group.getName()), jGroup);
      }
      jTask.put("counters", jGroups);
    }
  }

  /**
   * 修正Shuffle Errors计数器组的名称，改为全限定类名格式保持兼容性
   * @param name 原始组名
   * @return 修正后的组名
   */
  private String fixGroupNameForShuffleErrors(String name) {
    String retName = name;

    if (name.equals("Shuffle Errors")) {
      retName = "org.apache.hadoop.mapreduce.task.reduce.Fetcher.ShuffleErrors";
    }

    return retName;
  }
}