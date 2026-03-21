// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：GetApplicationsHomeSubClusterResponse 基于Protobuf的实现类，
 * 用于YARN联邦状态存储查询应用所属子集群响应的序列化与反序列化。
 */
@Private
@Unstable
public class GetApplicationsHomeSubClusterResponsePBImpl
    extends GetApplicationsHomeSubClusterResponse {

  // Protobuf 消息对象，当 viaProto 为 true 时持有原始消息
  private GetApplicationsHomeSubClusterResponseProto proto =
      GetApplicationsHomeSubClusterResponseProto.getDefaultInstance();
  // Protobuf 构建器，当 viaProto 为 false 时用于构建消息
  private GetApplicationsHomeSubClusterResponseProto.Builder builder = null;
  // 标识当前数据是否已经是 proto 格式
  private boolean viaProto = false;

  // 本地缓存的应用-所属子集群映射列表
  private List<ApplicationHomeSubCluster> appsHomeSubCluster;

  /**
   * 构造函数，初始化Protobuf构建器。
   */
  public GetApplicationsHomeSubClusterResponsePBImpl() {
    builder = GetApplicationsHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf消息构造响应对象。
   * @param proto 输入的Protobuf消息
   */
  public GetApplicationsHomeSubClusterResponsePBImpl(
      GetApplicationsHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf消息，合并本地修改。
   * @return 序列化后的Protobuf消息
   */
  public GetApplicationsHomeSubClusterResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存数据合并到Protobuf消息
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Protobuf构建器，如果当前是proto格式则基于现有proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationsHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的列表合并到Protobuf构建器
  private void mergeLocalToBuilder() {
    if (this.appsHomeSubCluster != null) {
      addSubClustersInfoToProto();
    }
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public List<ApplicationHomeSubCluster> getAppsHomeSubClusters() {
    initSubClustersInfoList();
    return appsHomeSubCluster;
  }

  @Override
  public void setAppsHomeSubClusters(
      List<ApplicationHomeSubCluster> appsHomeSubClusters) {
    maybeInitBuilder();
    if (appsHomeSubClusters == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    this.appsHomeSubCluster = appsHomeSubClusters;
    addSubClustersInfoToProto();
  }

  // 从Protobuf消息解析并初始化本地应用子集群列表缓存
  private void initSubClustersInfoList() {
    if (this.appsHomeSubCluster != null) {
      return;
    }
    GetApplicationsHomeSubClusterResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ApplicationHomeSubClusterProto> subClusterInfosList =
        p.getAppSubclusterMapList();
    appsHomeSubCluster = new ArrayList<>();

    for (ApplicationHomeSubClusterProto r : subClusterInfosList) {
      appsHomeSubCluster.add(convertFromProtoFormat(r));
    }
  }

  // 将本地应用子集群列表转换并写入Protobuf构建器
  private void addSubClustersInfoToProto() {
    maybeInitBuilder();
    builder.clearAppSubclusterMap();
    if (appsHomeSubCluster == null) {
      return;
    }
    // 自定义迭代器将API对象转换为Protobuf格式，流式写入构建器
    Iterable<ApplicationHomeSubClusterProto> iterable =
        new Iterable<ApplicationHomeSubClusterProto>() {
          @Override
          public Iterator<ApplicationHomeSubClusterProto> iterator() {
            return new Iterator<ApplicationHomeSubClusterProto>() {

              private Iterator<ApplicationHomeSubCluster> iter =
                  appsHomeSubCluster.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ApplicationHomeSubClusterProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllAppSubclusterMap(iterable);
  }

  /**
   * 将Protobuf格式转换为API对象。
   * @param sc Protobuf格式的应用子集群信息
   * @return API对象
   */
  private ApplicationHomeSubCluster convertFromProtoFormat(
      ApplicationHomeSubClusterProto sc) {
    return new ApplicationHomeSubClusterPBImpl(sc);
  }

  /**
   * 将API对象转换为Protobuf格式。
   * @param sc API格式的应用子集群信息
   * @return Protobuf对象
   */
  private ApplicationHomeSubClusterProto convertToProtoFormat(
      ApplicationHomeSubCluster sc) {
    return ((ApplicationHomeSubClusterPBImpl) sc).getProto();
  }

}