// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明: YARN联邦环境下，获取子集群信息响应的Protobuf实现类
 * 基于Protocol Buffer实现{@link GetSubClustersInfoResponse}接口。
 */
@Private
@Unstable
public class GetSubClustersInfoResponsePBImpl
    extends GetSubClustersInfoResponse {

  // Protobuf消息对象
  private GetSubClustersInfoResponseProto proto =
      GetSubClustersInfoResponseProto.getDefaultInstance();
  // Protobuf构建器
  private GetSubClustersInfoResponseProto.Builder builder = null;
  // 标识当前是否通过proto对象存储数据
  private boolean viaProto = false;

  // 缓存的子集群信息列表
  private List<SubClusterInfo> subClusterInfos;

  /**
   * 构造函数，初始化一个空的构建器。
   */
  public GetSubClustersInfoResponsePBImpl() {
    builder = GetSubClustersInfoResponseProto.newBuilder();
  }

  /**
   * 构造函数，基于已有proto对象构造实现。
   * @param proto 已构造好的proto对象
   */
  public GetSubClustersInfoResponsePBImpl(
      GetSubClustersInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的proto对象，合并本地缓存数据后返回。
   * @return 构建完成的proto对象
   */
  public GetSubClustersInfoResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的子集群信息合并到proton构建器中。
   */
  private void mergeLocalToBuilder() {
    if (this.subClusterInfos != null) {
      addSubClusterInfosToProto();
    }
  }

  /**
   * 将本地缓存数据合并到proto对象。
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
   * 如果当前是proto模式，初始化构建器。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClustersInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<SubClusterInfo> getSubClusters() {
    initSubClustersInfoList();
    return subClusterInfos;
  }

  @Override
  public void setSubClusters(Collection<SubClusterInfo> subClusters) {
    if (subClusters == null) {
      builder.clearSubClusterInfos();
      return;
    }
    this.subClusterInfos = subClusters.stream().collect(Collectors.toList());
    addSubClusterInfosToProto();
  }

  /**
   * 从proto对象中懒加载初始化子集群信息列表。
   */
  private void initSubClustersInfoList() {
    if (this.subClusterInfos != null) {
      return;
    }
    GetSubClustersInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<SubClusterInfoProto> subClusterInfosList = p.getSubClusterInfosList();
    subClusterInfos = new ArrayList<SubClusterInfo>();

    for (SubClusterInfoProto r : subClusterInfosList) {
      subClusterInfos.add(convertFromProtoFormat(r));
    }
  }

  /**
   * 将本地缓存的子集群信息写入proto构建器。
   */
  private void addSubClusterInfosToProto() {
    maybeInitBuilder();
    builder.clearSubClusterInfos();
    if (subClusterInfos == null) {
      return;
    }
    Iterable<SubClusterInfoProto> iterable =
        new Iterable<SubClusterInfoProto>() {
          @Override
          public Iterator<SubClusterInfoProto> iterator() {
            return new Iterator<SubClusterInfoProto>() {

              private Iterator<SubClusterInfo> iter =
                  subClusterInfos.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public SubClusterInfoProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllSubClusterInfos(iterable);
  }

  /**
   * 将业务对象转换为proto格式。
   * @param r 业务层子集群信息对象
   * @return proto格式子集群信息
   */
  private SubClusterInfoProto convertToProtoFormat(SubClusterInfo r) {
    return ((SubClusterInfoPBImpl) r).getProto();
  }

  /**
   * 将proto格式转换为业务对象。
   * @param r proto格式子集群信息
   * @return 业务层子集群信息对象
   */
  private SubClusterInfoPBImpl convertFromProtoFormat(SubClusterInfoProto r) {
    return new SubClusterInfoPBImpl(r);
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

}