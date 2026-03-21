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
package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;

import java.nio.ByteBuffer;

/**
 * RouterMasterKey的Protobuf实现类，用于YARN联邦状态存储中存储路由器主密钥信息，
 * 基于Protobuf实现序列化，支持在联邦状态存储中持久化读写。
 */
public class RouterMasterKeyPBImpl extends RouterMasterKey {

  // 持有的Protobuf对象实例
  private RouterMasterKeyProto proto = RouterMasterKeyProto.getDefaultInstance();
  // Protobuf构建器，用于构建修改对象
  private RouterMasterKeyProto.Builder builder = null;
  // 当前是否直接使用proto对象标识状态
  private boolean viaProto = false;

  /**
   * 构造函数，初始化空Builder用于构建新对象。
   */
  public RouterMasterKeyPBImpl() {
    builder = RouterMasterKeyProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf对象构造包装实例。
   * @param masterKeyProto 已构造好的RouterMasterKeyProto对象
   */
  public RouterMasterKeyPBImpl(RouterMasterKeyProto masterKeyProto) {
    this.proto = masterKeyProto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf实例，用于序列化。
   * @return 构造完成的RouterMasterKeyProto
   */
  public RouterMasterKeyProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 初始化Builder，确保可以修改对象，从现有proto拷贝构建Builder。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterMasterKeyProto.newBuilder(proto);
    }
    viaProto = false;
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
  public Integer getKeyId() {
    RouterMasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return p.getKeyId();
  }

  @Override
  public void setKeyId(Integer keyId) {
    maybeInitBuilder();
    if (keyId == null) {
      builder.clearKeyId();
      return;
    }
    builder.setKeyId(keyId);
  }

  @Override
  public ByteBuffer getKeyBytes() {
    RouterMasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getKeyBytes());
  }

  @Override
  public void setKeyBytes(ByteBuffer keyBytes) {
    maybeInitBuilder();
    if (keyBytes == null) {
      builder.clearKeyBytes();
      return;
    }
    builder.setKeyBytes(convertToProtoFormat(keyBytes));
  }

  @Override
  public Long getExpiryDate() {
    RouterMasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExpiryDate();
  }

  @Override
  public void setExpiryDate(Long expiryDate) {
    maybeInitBuilder();
    if (expiryDate == null) {
      builder.clearExpiryDate();
      return;
    }
    builder.setExpiryDate(expiryDate);
  }

  /**
   * 将Protobuf的ByteString转换为Java NIO ByteBuffer。
   * @param byteString Protobuf格式字节串
   * @return Java ByteBuffer
   */
  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  /**
   * 将Java NIO ByteBuffer转换为Protobuf的ByteString。
   * @param byteBuffer Java ByteBuffer
   * @return Protobuf格式字节串
   */
  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }
}