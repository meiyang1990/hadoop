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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * GetTaskReportsResponse协议的Protobuf实现类，封装获取任务报告响应的序列化与反序列化逻辑
 * 负责在MapReduce应用Master和客户端之间传递任务报告列表，继承ProtoBase实现Protobuf基础能力
 */    
public class GetTaskReportsResponsePBImpl extends ProtoBase<GetTaskReportsResponseProto> implements GetTaskReportsResponse {
  // Protobuf默认实例对象
  GetTaskReportsResponseProto proto = GetTaskReportsResponseProto.getDefaultInstance();
  // Protobuf构建器，用于构造对象
  GetTaskReportsResponseProto.Builder builder = null;
  // 当前是否通过Proto实例构建的标记
  boolean viaProto = false;
  
  // 缓存任务报告列表，避免重复反序列化
  private List<TaskReport> taskReports = null;
  
  
  /**
   * 默认构造函数，初始化空的Protobuf构建器
   */
  public GetTaskReportsResponsePBImpl() {
    builder = GetTaskReportsResponseProto.newBuilder();
  }

  /**
   * 通过已有的Protobuf对象构造封装类
   * @param proto 已反序列化好的GetTaskReportsResponseProto实例
   */
  public GetTaskReportsResponsePBImpl(GetTaskReportsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetTaskReportsResponseProto getProto() {
      // 将本地缓存的对象合并到Protobuf构建器
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的任务报告列表合并到Protobuf构建器
   */
  private void mergeLocalToBuilder() {
    if (this.taskReports != null) {
      addTaskReportsToProto();
    }
  }

  /**
   * 合并本地缓存到Protobuf对象，完成最终序列化
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 按需初始化Protobuf构建器，当通过现有Proto修改时需要先构建构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskReportsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public List<TaskReport> getTaskReportList() {
    initTaskReports();
    return this.taskReports;
  }
  
  @Override
  public TaskReport getTaskReport(int index) {
    initTaskReports();
    return this.taskReports.get(index);
  }
  
  @Override
  public int getTaskReportCount() {
    initTaskReports();
    return this.taskReports.size();
  }
  
  /**
   * 懒加载初始化任务报告列表，从Protobuf反序列化转换为API对象
   */
  private void initTaskReports() {
    if (this.taskReports != null) {
      return;
    }
    GetTaskReportsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskReportProto> list = p.getTaskReportsList();
    this.taskReports = new ArrayList<TaskReport>();

    for (TaskReportProto c : list) {
      this.taskReports.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllTaskReports(final List<TaskReport> taskReports) {
    if (taskReports == null)
      return;
    initTaskReports();
    this.taskReports.addAll(taskReports);
  }
  
  /**
   * 将本地缓存的任务报告列表转换并添加到Protobuf构建器
   */
  private void addTaskReportsToProto() {
    maybeInitBuilder();
    builder.clearTaskReports();
    if (taskReports == null)
      return;
    // 自定义可迭代对象，实现API对象到Protobuf对象的流式转换
    Iterable<TaskReportProto> iterable = new Iterable<TaskReportProto>() {
      @Override
      public Iterator<TaskReportProto> iterator() {
        return new Iterator<TaskReportProto>() {

          Iterator<TaskReport> iter = taskReports.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskReportProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllTaskReports(iterable);
  }
  
  @Override
  public void addTaskReport(TaskReport taskReports) {
    initTaskReports();
    this.taskReports.add(taskReports);
  }
  
  @Override
  public void removeTaskReport(int index) {
    initTaskReports();
    this.taskReports.remove(index);
  }
  
  @Override
  public void clearTaskReports() {
    initTaskReports();
    this.taskReports.clear();
  }

  /**
   * 将Protobuf格式的任务报告转换为API对象
   * @param p Protobuf格式的任务报告
   * @return API层的任务报告对象
   */
  private TaskReportPBImpl convertFromProtoFormat(TaskReportProto p) {
    return new TaskReportPBImpl(p);
  }

  /**
   * 将API层的任务报告转换为Protobuf格式
   * @param t API层的任务报告对象
   * @return Protobuf格式的任务报告
   */
  private TaskReportProto convertToProtoFormat(TaskReport t) {
    return ((TaskReportPBImpl)t).getProto();
  }



}