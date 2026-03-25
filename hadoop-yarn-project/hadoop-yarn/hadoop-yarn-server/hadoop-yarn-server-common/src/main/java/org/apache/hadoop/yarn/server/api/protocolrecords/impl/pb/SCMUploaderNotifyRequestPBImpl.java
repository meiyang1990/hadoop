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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;

/**
 * SCM上传通知请求的Protobuf实现类，用于节点管理器向共享缓存管理器通知
 * 有新的应用缓存资源上传完成。
 */
public class SCMUploaderNotifyRequestPBImpl extends SCMUploaderNotifyRequest {
  // Protobuf消息对象
  SCMUploaderNotifyRequestProto proto =
      SCMUploaderNotifyRequestProto.getDefaultInstance();
  // Protobuf消息构造器
  SCMUploaderNotifyRequestProto.Builder builder = null;
  // 标记是否通过已有proto构建
  boolean viaProto = false;

  /**
   * 空构造函数，初始化Protobuf构造器。
   */
  public SCMUploaderNotifyRequestPBImpl() {
    builder = SCMUploaderNotifyRequestProto.newBuilder();
  }

  /**
   * 通过已有Protobuf对象构造请求实例。
   * @param proto 已有的Protobuf请求对象
   */
  public SCMUploaderNotifyRequestPBImpl(
      SCMUploaderNotifyRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取构建完成的Protobuf请求对象。
   * @return 构建完成的Protobuf请求
   */
  public SCMUploaderNotifyRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public String getResourceKey() {
    SCMUploaderNotifyRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasResourceKey()) ? p.getResourceKey() : null;
  }

  @Override
  public void setResourceKey(String key) {
    maybeInitBuilder();
    if (key == null) {
      builder.clearResourceKey();
      return;
    }
    builder.setResourceKey(key);
  }

  @Override
  public String getFileName() {
    SCMUploaderNotifyRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasFilename()) ? p.getFilename() : null;
  }

  @Override
  public void setFilename(String filename) {
    maybeInitBuilder();
    if (filename == null) {
      builder.clearFilename();
      return;
    }
    builder.setFilename(filename);
  }

  /**
   * 将本地修改合并到Protobuf对象中。
   */
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果需要则初始化构造器，从已有proto拷贝。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SCMUploaderNotifyRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
}