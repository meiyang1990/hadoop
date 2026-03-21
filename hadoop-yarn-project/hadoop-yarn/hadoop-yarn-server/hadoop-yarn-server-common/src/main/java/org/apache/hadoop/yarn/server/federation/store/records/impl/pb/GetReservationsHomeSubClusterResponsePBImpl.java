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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationsHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationsHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：基于Protocol Buffer实现的{@link GetReservationsHomeSubClusterResponse}，用于YARN联邦存储查询预约归属子集群响应的PB序列化实现
 * GetReservationsHomeSubClusterResponse基于Protocol Buffer的实现类，处理YARN联邦预约查询响应的序列化。
 */
@Private
@Unstable
public class GetReservationsHomeSubClusterResponsePBImpl
    extends GetReservationsHomeSubClusterResponse {

  // PB协议对象，存储序列化后的响应数据
  private GetReservationsHomeSubClusterResponseProto proto =
      GetReservationsHomeSubClusterResponseProto.getDefaultInstance();
  // PB构建器，用于构建响应对象
  private GetReservationsHomeSubClusterResponseProto.Builder builder = null;
  // 标记当前是否通过proto对象存储数据
  private boolean viaProto = false;

  // 本地缓存的预约归属子集群列表
  private List<ReservationHomeSubCluster> appsHomeSubCluster;

  /**
   * 构造函数，初始化空的PB构建器。
   */
  public GetReservationsHomeSubClusterResponsePBImpl() {
    builder = GetReservationsHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 构造函数，基于已有proto对象构建响应实例。
   * @param proto 已有的proto响应对象
   */
  public GetReservationsHomeSubClusterResponsePBImpl(
      GetReservationsHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应的proto对象，合并本地缓存数据到proto后返回。
   * @return 序列化后的proto对象
   */
  public GetReservationsHomeSubClusterResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 合并本地缓存数据到proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化PB构建器，若当前基于proto存储则复制现有数据到构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetReservationsHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地缓存数据到PB构建器
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
  public List<ReservationHomeSubCluster> getAppsHomeSubClusters() {
    initSubClustersInfoList();
    return appsHomeSubCluster;
  }

  @Override
  public void setAppsHomeSubClusters(
      List<ReservationHomeSubCluster> appsHomeSubClusters) {
    maybeInitBuilder();
    if (appsHomeSubClusters == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    this.appsHomeSubCluster = appsHomeSubClusters;
    addSubClustersInfoToProto();
  }

  // 从PB对象初始化本地预约归属子集群列表缓存
  private void initSubClustersInfoList() {
    if (this.appsHomeSubCluster != null) {
      return;
    }
    GetReservationsHomeSubClusterResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ReservationHomeSubClusterProto> subClusterInfosList = p.getAppSubclusterMapList();
    appsHomeSubCluster = new ArrayList<>();

    for (ReservationHomeSubClusterProto r : subClusterInfosList) {
      appsHomeSubCluster.add(convertFromProtoFormat(r));
    }
  }

  // 将本地缓存的预约归属子集群列表转换写入PB构建器
  private void addSubClustersInfoToProto() {
    maybeInitBuilder();
    builder.clearAppSubclusterMap();
    if (appsHomeSubCluster == null) {
      return;
    }
    Iterable<ReservationHomeSubClusterProto> iterable =
        new Iterable<ReservationHomeSubClusterProto>() {
          @Override
          public Iterator<ReservationHomeSubClusterProto> iterator() {
            return new Iterator<ReservationHomeSubClusterProto>() {

              private Iterator<ReservationHomeSubCluster> iter = appsHomeSubCluster.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ReservationHomeSubClusterProto next() {
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

  // 将PB格式预约归属对象转换为业务对象
  private ReservationHomeSubCluster convertFromProtoFormat(ReservationHomeSubClusterProto sc) {
    return new ReservationHomeSubClusterPBImpl(sc);
  }

  // 将业务格式预约归属对象转换为PB对象
  private ReservationHomeSubClusterProto convertToProtoFormat(ReservationHomeSubCluster sc) {
    return ((ReservationHomeSubClusterPBImpl) sc).getProto();
  }
}