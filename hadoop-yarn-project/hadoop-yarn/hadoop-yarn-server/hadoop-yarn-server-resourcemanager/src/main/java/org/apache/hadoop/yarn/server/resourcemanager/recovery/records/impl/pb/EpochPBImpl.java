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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProtoOrBuilder;


import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.Epoch;

/**
 * Epoch记录的Protobuf实现，用于RM恢复场景下存储并序列化 epoch 信息，
 * 基于Protobuf序列化格式实现，支持持久化存储与网络传输。
 */
public class EpochPBImpl extends Epoch {

  // Protobuf对象实例
  EpochProto proto = EpochProto.getDefaultInstance();
  // Protobuf构建器，用于修改对象时构建实例
  EpochProto.Builder builder = null;
  // 当前是否使用已构建的proto实例
  boolean viaProto = false;

  /**
   * 无参构造函数，初始化Builder用于构造新的Epoch对象。
   */
  public EpochPBImpl() {
    builder = EpochProto.newBuilder();
  }

  /**
   * 通过现有Protobuf对象构造Epoch封装实例。
   * @param proto 已有的EpochProto对象
   */
  public EpochPBImpl(EpochProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf实例，用于序列化。
   * @return 构建完成的EpochProto对象
   */
  public EpochProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 延迟初始化Builder：如果当前使用的是只读proto实例，
   * 则基于现有proto构建Builder，准备进行修改操作。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = EpochProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getEpoch() {
    EpochProtoOrBuilder p = viaProto ? proto : builder;
    return p.getEpoch();
  }

  @Override
  public void setEpoch(long sequentialNumber) {
    maybeInitBuilder();
    builder.setEpoch(sequentialNumber);
  }

}