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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;

/**
 * SCM上传权限检查响应的Protobuf实现类，基于PB序列化协议封装响应数据。
 * 用于Shared Cache Manager中客户端查询是否允许上传缓存归档时的服务端响应。
 */
public class SCMUploaderCanUploadResponsePBImpl
    extends SCMUploaderCanUploadResponse {
  // 存储序列化后的Protobuf对象实例
  SCMUploaderCanUploadResponseProto proto =
      SCMUploaderCanUploadResponseProto.getDefaultInstance();
  // Protobuf构建器，用于构建/修改响应对象
  SCMUploaderCanUploadResponseProto.Builder builder = null;
  // 标记当前是否通过已构建的Proto对象访问数据
  boolean viaProto = false;

  /**
   * 构造空响应对象，初始化Protobuf构建器。
   */
  public SCMUploaderCanUploadResponsePBImpl() {
    builder = SCMUploaderCanUploadResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应包装类。
   * @param proto 已构建的Protobuf响应对象
   */
  public SCMUploaderCanUploadResponsePBImpl(
      SCMUploaderCanUploadResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的Protobuf对象，合并本地修改后返回。
   * @return 序列化后的Protobuf响应对象
   */
  public SCMUploaderCanUploadResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public boolean getUploadable() {
    // 根据当前状态选择读取Proto还是Builder中的数据
    SCMUploaderCanUploadResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 默认允许上传，不确定时返回true保持兼容性
    return (p.hasUploadable()) ? p.getUploadable() : true;
  }

  @Override
  public void setUploadable(boolean b) {
    maybeInitBuilder();
    builder.setUploadable(b);
  }

  // 将本地修改合并到Proto对象中
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Builder：如果当前是通过Proto访问，基于现有Proto构建Builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SCMUploaderCanUploadResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}