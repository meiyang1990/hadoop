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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;

/**
 * .NodeManager注销响应的Protobuf实现类，基于PB序列化协议实现协议记录
 */
public class UnRegisterNodeManagerResponsePBImpl extends
    UnRegisterNodeManagerResponse {
  // Protobuf消息对象，用于只读模式
  private UnRegisterNodeManagerResponseProto proto =
      UnRegisterNodeManagerResponseProto.getDefaultInstance();
  // Protobuf构建器，用于可写模式
  private UnRegisterNodeManagerResponseProto.Builder builder = null;
  // 当前是否使用只读proto对象标记
  private boolean viaProto = false;

  // 是否需要重新构建proto对象标记
  private boolean rebuild = false;

  /**
   * 构造空的注销响应对象，初始化构建器.
   */
  public UnRegisterNodeManagerResponsePBImpl() {
    builder = UnRegisterNodeManagerResponseProto.newBuilder();
  }

  /**
   * 基于已有proto对象构造注销响应对象.
   * @param proto 已有的Protobuf消息对象
   */
  public UnRegisterNodeManagerResponsePBImpl(
      UnRegisterNodeManagerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的Protobuf消息对象，处理构建逻辑.
   * @return 构建完成的Protobuf消息对象
   */
  public UnRegisterNodeManagerResponseProto getProto() {
    if (rebuild) {
      mergeLocalToProto();
    }
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地构建器内容合并生成新的proto对象.
   */
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  /**
   * 延迟初始化构建器：如果当前是proto只读模式，基于现有proto创建构建器.
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UnRegisterNodeManagerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}