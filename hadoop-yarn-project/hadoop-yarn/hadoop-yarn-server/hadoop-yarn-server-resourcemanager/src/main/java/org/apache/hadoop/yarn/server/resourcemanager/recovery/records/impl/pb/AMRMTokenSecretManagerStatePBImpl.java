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

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;

/**
 * AMRMTokenSecretManager状态的Protobuf实现类，用于RM恢复场景下持久化存储密钥状态
 */
public class AMRMTokenSecretManagerStatePBImpl extends AMRMTokenSecretManagerState{
  /** Protobuf对象实例 */
  AMRMTokenSecretManagerStateProto proto =
      AMRMTokenSecretManagerStateProto.getDefaultInstance();
  /** Protobuf Builder */
  AMRMTokenSecretManagerStateProto.Builder builder = null;
  /** 标识当前数据是否来自Proto对象 */
  boolean viaProto = false;

  /** 当前生效的主密钥 */
  private MasterKey currentMasterKey = null;
  /** 即将生效的下一代主密钥 */
  private MasterKey nextMasterKey = null;

  /**
   * 构造函数，初始化Builder用于构建新对象
   */
  public AMRMTokenSecretManagerStatePBImpl() {
    builder = AMRMTokenSecretManagerStateProto.newBuilder();
  }

  /**
   * 构造函数，从已有的Proto对象解析
   * @param proto 已构造完成的Proto对象
   */
  public AMRMTokenSecretManagerStatePBImpl(AMRMTokenSecretManagerStateProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取最终构造的Proto对象，自动合并本地修改
   * @return 完整的Proto对象
   */
  public AMRMTokenSecretManagerStateProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的对象合并到Protobuf Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.currentMasterKey != null) {
      builder.setCurrentMasterKey(convertToProtoFormat(this.currentMasterKey));
    }
    if (this.nextMasterKey != null) {
      builder.setNextMasterKey(convertToProtoFormat(this.nextMasterKey));
    }
  }

  /**
   * 将本地修改合并到Proto对象中，生成最终完整Proto
   */
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果需要，基于现有Proto初始化Builder
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AMRMTokenSecretManagerStateProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public MasterKey getCurrentMasterKey() {
    AMRMTokenSecretManagerStateProtoOrBuilder p = viaProto ? proto : builder;
    // 优先返回本地缓存
    if (this.currentMasterKey != null) {
      return this.currentMasterKey;
    }
    // Proto中不存在则返回空
    if (!p.hasCurrentMasterKey()) {
      return null;
    }
    // 从Proto解析并缓存
    this.currentMasterKey = convertFromProtoFormat(p.getCurrentMasterKey());
    return this.currentMasterKey;
  }

  @Override
  public void setCurrentMasterKey(MasterKey currentMasterKey) {
    maybeInitBuilder();
    // 空值清空对应字段
    if (currentMasterKey == null)
      builder.clearCurrentMasterKey();
    this.currentMasterKey = currentMasterKey;
  }

  @Override
  public MasterKey getNextMasterKey() {
    AMRMTokenSecretManagerStateProtoOrBuilder p = viaProto ? proto : builder;
    // 优先返回本地缓存
    if (this.nextMasterKey != null) {
      return this.nextMasterKey;
    }
    // Proto中不存在则返回空
    if (!p.hasNextMasterKey()) {
      return null;
    }
    // 从Proto解析并缓存
    this.nextMasterKey = convertFromProtoFormat(p.getNextMasterKey());
    return this.nextMasterKey;
  }

  @Override
  public void setNextMasterKey(MasterKey nextMasterKey) {
    maybeInitBuilder();
    // 空值清空对应字段
    if (nextMasterKey == null)
      builder.clearNextMasterKey();
    this.nextMasterKey = nextMasterKey;
  }

  /**
   * 将MasterKey领域对象转换为Protobuf格式
   * @param t 领域对象
   * @return Protobuf对象
   */
  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl) t).getProto();
  }

  /**
   * 将Protobuf对象转换为MasterKey领域对象
   * @param p Protobuf对象
   * @return 领域对象
   */
  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }
}