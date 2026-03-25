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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;

/**
 * SCM上传通知响应的Protobuf实现类，基于PB协议序列化响应数据。
 * 用于共享缓存管理器(SCM)处理文件上传通知后返回结果。
 */
public class SCMUploaderNotifyResponsePBImpl extends SCMUploaderNotifyResponse {
  // Protobuf消息对象
  SCMUploaderNotifyResponseProto proto =
      SCMUploaderNotifyResponseProto.getDefaultInstance();
  // Protobuf消息构建器
  SCMUploaderNotifyResponseProto.Builder builder = null;
  // 是否通过proto对象直接访问（false表示正在通过builder修改）
  boolean viaProto = false;

  /**
   * 构造空的SCM上传通知响应对象，使用Builder模式初始化。
   */
  public SCMUploaderNotifyResponsePBImpl() {
    builder = SCMUploaderNotifyResponseProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf消息构造响应对象。
   * @param proto 已构造完成的Protobuf响应消息
   */
  public SCMUploaderNotifyResponsePBImpl(SCMUploaderNotifyResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取序列化后的Protobuf响应消息，合并本地修改后生成最终对象。
   * @return 序列化完成的Protobuf响应消息
   */
  public SCMUploaderNotifyResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public boolean getAccepted() {
    // 根据当前状态选择读取proto还是builder
    SCMUploaderNotifyResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 不确定时默认接受，保留文件在缓存中
    return (p.hasAccepted()) ? p.getAccepted() : true;
  }

  @Override
  public void setAccepted(boolean b) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 设置接受状态
    builder.setAccepted(b);
  }

  /**
   * 将本地builder的修改合并到proto对象中。
   */
  private void mergeLocalToProto() {
    if (viaProto)
      // 如果当前基于proto，先初始化builder加载proto内容
      maybeInitBuilder();
    // 构建新的proto对象
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 按需初始化builder，若当前基于proto则将proto内容拷贝到builder中。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      // 基于现有proto创建新的builder，拷贝已有内容
      builder = SCMUploaderNotifyResponseProto.newBuilder(proto);
    }
    // 标记当前正在通过builder修改
    viaProto = false;
  }
}