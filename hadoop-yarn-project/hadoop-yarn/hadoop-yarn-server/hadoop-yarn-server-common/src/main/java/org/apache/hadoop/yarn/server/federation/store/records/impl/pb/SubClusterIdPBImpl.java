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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * 文件级：YARN联邦子集群ID的Protocol Buffer实现类，基于PB序列化机制实现
 * Protocol buffer based implementation of {@link SubClusterId}.
 */
@Private
@Unstable
public class SubClusterIdPBImpl extends SubClusterId {

  // 存储子集群ID的PB proto对象
  private SubClusterIdProto proto = SubClusterIdProto.getDefaultInstance();
  // PB构建器，用于构造修改proto对象
  private SubClusterIdProto.Builder builder = null;
  // 标识当前是否使用已构建完成的proto对象，false表示正在使用builder构造
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化PB构建器
   */
  public SubClusterIdPBImpl() {
    builder = SubClusterIdProto.newBuilder();
  }

  /**
   * 基于已有proto对象构造实例
   * @param proto 已构造完成的子集群ID proto对象
   */
  public SubClusterIdPBImpl(SubClusterIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的proto对象，统一序列化出口
   * @return 子集群ID proto对象
   */
  public SubClusterIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 确保builder已初始化，用于修改操作前准备
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getId() {
    SubClusterIdProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  protected void setId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearId();
      return;
    }
    builder.setId(subClusterId);
  }

}