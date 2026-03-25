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
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取诊断信息响应的Protobuf实现，基于ProtoBase实现MapReduce服务协议中的响应对象
 * 负责处理客户端查询作业/任务诊断信息时，服务端响应的PB序列化与反序列化
 */    
public class GetDiagnosticsResponsePBImpl extends ProtoBase<GetDiagnosticsResponseProto> implements GetDiagnosticsResponse {
  // Protobuf消息对象，当通过proto构建时使用
  GetDiagnosticsResponseProto proto = GetDiagnosticsResponseProto.getDefaultInstance();
  // Protobuf构建器，当本地修改数据时使用
  GetDiagnosticsResponseProto.Builder builder = null;
  // 标识当前数据是否来自proto对象
  boolean viaProto = false;
  
  // 本地缓存的诊断信息列表
  private List<String> diagnostics = null;
  
  
  /**
   * 构造空的获取诊断信息响应对象
   */
  public GetDiagnosticsResponsePBImpl() {
    builder = GetDiagnosticsResponseProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf消息构造响应对象
   * @param proto 已构造好的GetDiagnosticsResponseProto对象
   */
  public GetDiagnosticsResponsePBImpl(GetDiagnosticsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetDiagnosticsResponseProto getProto() {
      // 合并本地修改到proto
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 合并本地缓存的诊断信息到Protobuf构建器
  private void mergeLocalToBuilder() {
    if (this.diagnostics != null) {
      addDiagnosticsToProto();
    }
  }

  // 合并本地修改，生成最终的Protobuf消息对象
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如果当前从proto读取数据，初始化Protobuf构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetDiagnosticsResponseProto.newBuilder(proto);
    }
    viaProto = false;
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
  
  // 从Protobuf消息初始化本地诊断信息列表
  private void initDiagnostics() {
    if (this.diagnostics != null) {
      return;
    }
    GetDiagnosticsResponseProtoOrBuilder p = viaProto ? proto : builder;
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
  
  // 将本地诊断信息列表写入Protobuf构建器
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

}