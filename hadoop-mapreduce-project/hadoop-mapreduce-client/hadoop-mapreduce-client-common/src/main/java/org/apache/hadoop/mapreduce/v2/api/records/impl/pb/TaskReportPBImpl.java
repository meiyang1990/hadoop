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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskStateProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * TaskReport的Protobuf实现，基于ProtoBase封装，用于MapReduce协议中任务报告的序列化与反序列化
 * 维护任务运行状态、进度、计数器、诊断信息等任务执行信息
 */
public class TaskReportPBImpl extends ProtoBase<TaskReportProto> implements TaskReport {
  TaskReportProto proto = TaskReportProto.getDefaultInstance();
  TaskReportProto.Builder builder = null;
  boolean viaProto = false;

  private TaskId taskId = null;
  private Counters counters = null;
  private org.apache.hadoop.mapreduce.Counters rawCounters = null;
  private List<TaskAttemptId> runningAttempts = null;
  private TaskAttemptId successfulAttemptId = null;
  private List<String> diagnostics = null;
  private String status;

  /**
   * 构造空的TaskReportPBImpl，初始化Builder
   */
  public TaskReportPBImpl() {
    builder = TaskReportProto.newBuilder();
  }

  /**
   * 基于已有的TaskReportProto构造TaskReportPBImpl
   * @param proto 已构造完成的TaskReportProto对象
   */
  public TaskReportPBImpl(TaskReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public TaskReportProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的Java对象合并到Proto Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
    convertRawCountersToCounters();
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
    if (this.runningAttempts != null) {
      addRunningAttemptsToProto();
    }
    if (this.successfulAttemptId != null) {
      builder.setSuccessfulAttempt(convertToProtoFormat(this.successfulAttemptId));
    }
    if (this.diagnostics != null) {
      addDiagnosticsToProto();
    }
  }

  /**
   * 将本地修改合并生成最终的Proto对象
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前基于Proto读取，初始化Builder用于修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Counters getCounters() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    convertRawCountersToCounters();
    if (this.counters != null) {
      return this.counters;
    }
    if (!p.hasCounters()) {
      return null;
    }
    this.counters = convertFromProtoFormat(p.getCounters());
    return this.counters;
  }

  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) {
      builder.clearCounters();
    }
    this.counters = counters;
    this.rawCounters = null;
  }

  @Override
  public org.apache.hadoop.mapreduce.Counters
      getRawCounters() {
    return this.rawCounters;
  }

  @Override
  public void setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters) {
    setCounters(null);
    this.rawCounters = rCounters;
  }

  /**
   * 将旧API格式的原始计数器转换为YARN新API格式
   */
  private void convertRawCountersToCounters() {
    if (this.counters == null && this.rawCounters != null) {
      this.counters = TypeConverter.toYarn(rawCounters);
      this.rawCounters = null;
    }
  }

  @Override
  public long getStartTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }
  
  @Override
  public long getFinishTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }
  
  @Override
  public TaskId getTaskId() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    this.taskId = convertFromProtoFormat(p.getTaskId());
    return this.taskId;
  }

  @Override
  public void setTaskId(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null) 
      builder.clearTaskId();
    this.taskId = taskId;
  }
  @Override
  public float getProgress() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getProgress());
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress((progress));
  }

  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public TaskState getTaskState() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskState()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskState());
  }

  @Override
  public void setTaskState(TaskState taskState) {
    maybeInitBuilder();
    if (taskState == null) {
      builder.clearTaskState();
      return;
    }
    builder.setTaskState(convertToProtoFormat(taskState));
  }
  @Override
  public List<TaskAttemptId> getRunningAttemptsList() {
    initRunningAttempts();
    return this.runningAttempts;
  }
  @Override
  public TaskAttemptId getRunningAttempt(int index) {
    initRunningAttempts();
    return this.runningAttempts.get(index);
  }
  @Override
  public int getRunningAttemptsCount() {
    initRunningAttempts();
    return this.runningAttempts.size();
  }
  
  /**
   * 从Proto中懒加载初始化正在运行的尝试尝试列表
   */
  private void initRunningAttempts() {
    if (this.runningAttempts != null) {
      return;
    }
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskAttemptIdProto> list = p.getRunningAttemptsList();
    this.runningAttempts = new ArrayList<TaskAttemptId>();

    for (TaskAttemptIdProto c : list) {
      this.runningAttempts.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllRunningAttempts(final List<TaskAttemptId> runningAttempts) {
    if (runningAttempts == null)
      return;
    initRunningAttempts();
    this.runningAttempts.addAll(runningAttempts);
  }
  
  /**
   * 将本地内存中的运行尝试列表写入Proto Builder
   */
  private void addRunningAttemptsToProto() {
    maybeInitBuilder();
    builder.clearRunningAttempts();
    if (runningAttempts == null)
      return;
    Iterable<TaskAttemptIdProto> iterable = new Iterable<TaskAttemptIdProto>() {
      @Override
      public Iterator<TaskAttemptIdProto> iterator() {
        return new Iterator<TaskAttemptIdProto>() {

          Iterator<TaskAttemptId> iter = runningAttempts.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskAttemptIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllRunningAttempts(iterable);
  }
  @Override
  public void addRunningAttempt(TaskAttemptId runningAttempts) {
    initRunningAttempts();
    this.runningAttempts.add(runningAttempts);
  }
  @Override
  public void removeRunningAttempt(int index) {
    initRunningAttempts();
    this.runningAttempts.remove(index);
  }
  @Override
  public void clearRunningAttempts() {
    initRunningAttempts();
    this.runningAttempts.clear();
  }
  @Override
  public TaskAttemptId getSuccessfulAttempt() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.successfulAttemptId != null) {
      return this.successfulAttemptId;
    }
    if (!p.hasSuccessfulAttempt()) {
      return null;
    }
    this.successfulAttemptId = convertFromProtoFormat(p.getSuccessfulAttempt());
    return this.successfulAttemptId;
  }

  @Override
  public void setSuccessfulAttempt(TaskAttemptId successfulAttempt) {
    maybeInitBuilder();
    if (successfulAttempt == null) 
      builder.clearSuccessfulAttempt();
    this.successfulAttemptId = successfulAttempt;
  }
  @Override
  public List<String> getDiagnosticsList() {
    initDiagnostics();
    return this.diagnostics;
  }
  @Override
  public String getDiagnostics(int index) {
    initDiagnostics();
    return this.diagnostics.get(index);
  }
  @Override
  public int getDiagnosticsCount() {
    initDiagnostics();
    return this.diagnostics.size();
  }
  
  /**
   * 从Proto中懒加载初始化诊断信息列表
   */
  private void initDiagnostics() {
    if (this.diagnostics != null) {
      return;
    }
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getDiagnosticsList();
    this.diagnostics = new ArrayList<String>();

    for (String c : list) {
      this.diagnostics.add(c);
    }
  }
  
  @Override
  public void addAllDiagnostics(final List<String> diagnostics) {
    if (diagnostics == null)
      return;
    initDiagnostics();
    this.diagnostics.addAll(diagnostics);
  }
  
  /**
   * 将本地内存中的诊断信息写入Proto Builder
   */
  private void addDiagnosticsToProto() {
    maybeInitBuilder();
    builder.clearDiagnostics();
    if (diagnostics == null) 
      return;
    builder.addAllDiagnostics(diagnostics);
  }
  @Override
  public void addDiagnostics(String diagnostics) {
    initDiagnostics();
    this.diagnostics.add(diagnostics);
  }
  @Override
  public void removeDiagnostics(int index) {
    initDiagnostics();
    this.diagnostics.remove(index);
  }
  @Override
  public void clearDiagnostics() {
    initDiagnostics();
    this.diagnostics.clear();
  }

  /**
   * 将Protobuf格式Counters转换为Java对象
   */
  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  /**
   * 将Java对象格式Counters转换为Protobuf格式
   */
  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }

  /**
   * 将Protobuf格式TaskId转换为Java对象
   */
  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  /**
   * 将Java对象格式TaskId转换为Protobuf格式
   */
  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }

  /**
   * 将枚举TaskState转换为Protobuf枚举格式
   */
  private TaskStateProto convertToProtoFormat(TaskState e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  /**
   * 将Protobuf枚举格式TaskState转换为Java枚举
   */
  private TaskState convertFromProtoFormat(TaskStateProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

  /**
   * 将Protobuf格式TaskAttemptId转换为Java对象
   */
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  /**
   * 将Java对象格式TaskAttemptId转换为Protobuf格式
   */
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }



}