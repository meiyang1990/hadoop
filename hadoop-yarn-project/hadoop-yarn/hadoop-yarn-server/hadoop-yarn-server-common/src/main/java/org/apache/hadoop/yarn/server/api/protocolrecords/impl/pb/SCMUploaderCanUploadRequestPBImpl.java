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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;

/**
 * SCM上传许可请求的Protobuf实现类，用于节点共享缓存服务中请求检查是否可上传指定资源。
 * 基于Protobuf序列化框架实现协议数据的封装转换。
 */
public class SCMUploaderCanUploadRequestPBImpl
    extends SCMUploaderCanUploadRequest {
  // 存储Protobuf消息实例
  SCMUploaderCanUploadRequestProto proto =
      SCMUploaderCanUploadRequestProto.getDefaultInstance();
  // Protobuf消息构建器，用于构建修改请求
  SCMUploaderCanUploadRequestProto.Builder builder = null;
  // 标记当前是否通过已构建的Protobuf实例持有数据
  boolean viaProto = false;

  public SCMUploaderCanUploadRequestPBImpl() {
    builder = SCMUploaderCanUploadRequestProto.newBuilder();
  }

  public SCMUploaderCanUploadRequestPBImpl(
      SCMUploaderCanUploadRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf对象，合并本地修改后返回。
   * @return 序列化后的Protobuf请求对象
   */
  public SCMUploaderCanUploadRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public String getResourceKey() {
    SCMUploaderCanUploadRequestProtoOrBuilder p = viaProto ? proto : builder;
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

  // 将本地构建的修改合并到Protobuf实例中
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化构建器，若当前持有已构建的Protobuf实例，基于它创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SCMUploaderCanUploadRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
}