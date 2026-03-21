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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.impl.pb.AppCollectorDataPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppCollectorDataProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;

/**
 * ReportNewCollectorInfoRequest的Protobuf实现类，
 * 用于序列化/反序列化上报新Collector信息的请求，基于Protobuf协议实现RPC传输
 */
public class ReportNewCollectorInfoRequestPBImpl extends
    ReportNewCollectorInfoRequest {

  // Protobuf消息对象，只读模式下使用
  private ReportNewCollectorInfoRequestProto proto =
      ReportNewCollectorInfoRequestProto.getDefaultInstance();

  // Protobuf消息构造器，可写模式下使用
  private ReportNewCollectorInfoRequestProto.Builder builder = null;
  // 标识当前是否通过只读proto存储数据
  private boolean viaProto = false;

  // 本地缓存的Collector信息列表
  private List<AppCollectorData> collectorsList = null;

  public ReportNewCollectorInfoRequestPBImpl() {
    builder = ReportNewCollectorInfoRequestProto.newBuilder();
  }

  public ReportNewCollectorInfoRequestPBImpl(
      ReportNewCollectorInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的Protobuf消息对象，合并本地修改并构建最终消息
   * @return 构建完成的ReportNewCollectorInfoRequestProto对象
   */
  public ReportNewCollectorInfoRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  /**
   * 将本地修改合并到proto对象中
   */
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 将本地字段合并到Protobuf构造器中
   */
  private void mergeLocalToBuilder() {
    if (collectorsList != null) {
      addLocalCollectorsToProto();
    }
  }

  /**
   * 如果当前是只读proto模式，初始化可写构造器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReportNewCollectorInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的Collector列表转换为Protobuf格式添加到构造器
   */
  private void addLocalCollectorsToProto() {
    maybeInitBuilder();
    builder.clearAppCollectors();
    List<AppCollectorDataProto> protoList =
        new ArrayList<AppCollectorDataProto>();
    for (AppCollectorData m : this.collectorsList) {
      protoList.add(convertToProtoFormat(m));
    }
    builder.addAllAppCollectors(protoList);
  }

  /**
   * 从Protobuf解析Collector列表到本地缓存
   */
  private void initLocalCollectorsList() {
    ReportNewCollectorInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<AppCollectorDataProto> list =
        p.getAppCollectorsList();
    this.collectorsList = new ArrayList<AppCollectorData>();
    for (AppCollectorDataProto m : list) {
      this.collectorsList.add(convertFromProtoFormat(m));
    }
  }

  @Override
  public List<AppCollectorData> getAppCollectorsList() {
    if (this.collectorsList == null) {
      initLocalCollectorsList();
    }
    return this.collectorsList;
  }

  @Override
  public void setAppCollectorsList(List<AppCollectorData> appCollectorsList) {
    maybeInitBuilder();
    if (appCollectorsList == null) {
      builder.clearAppCollectors();
    }
    this.collectorsList = appCollectorsList;
  }

  /**
   * 将Protobuf格式转换为内部API格式
   * @param p Protobuf格式的Collector数据
   * @return 内部API格式的Collector数据
   */
  private AppCollectorDataPBImpl convertFromProtoFormat(
      AppCollectorDataProto p) {
    return new AppCollectorDataPBImpl(p);
  }

  /**
   * 将内部API格式转换为Protobuf格式
   * @param m 内部API格式的Collector数据
   * @return Protobuf格式的Collector数据
   */
  private AppCollectorDataProto convertToProtoFormat(
      AppCollectorData m) {
    return ((AppCollectorDataPBImpl) m).getProto();
  }

}