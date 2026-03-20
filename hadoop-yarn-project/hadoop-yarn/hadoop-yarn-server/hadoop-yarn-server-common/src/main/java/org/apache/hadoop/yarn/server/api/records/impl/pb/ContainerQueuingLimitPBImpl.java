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

package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ContainerQueuingLimitProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ContainerQueuingLimitProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;

/**
 * Implementation of ContainerQueuingLimit interface.
 */
// ContainerQueuingLimit 的 PB 实现，负责在 builder 与 proto 间同步队列限制
public class ContainerQueuingLimitPBImpl extends ContainerQueuingLimit {

  private ContainerQueuingLimitProto proto =
      ContainerQueuingLimitProto.getDefaultInstance();
  private ContainerQueuingLimitProto.Builder builder = null;
  private boolean viaProto = false;

  // 默认创建 builder，供上层设置排队限制
  public ContainerQueuingLimitPBImpl() {
    builder = ContainerQueuingLimitProto.newBuilder();
  }

  // 用已有 proto 包装，读取时懒加载
  public ContainerQueuingLimitPBImpl(ContainerQueuingLimitProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  // 返回最新 proto，必要时从 builder 构造
  public ContainerQueuingLimitProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return  proto;
  }

  // 确保处于可写模式；若当前持有的是 proto，则基于它创建 builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerQueuingLimitProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getMaxQueueWaitTimeInMs() {
    ContainerQueuingLimitProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMaxQueueWaitTimeInMs();
  }

  @Override
  public void setMaxQueueWaitTimeInMs(int waitTime) {
    maybeInitBuilder();
    builder.setMaxQueueWaitTimeInMs(waitTime);
  }

  @Override
  public int getMaxQueueLength() {
    ContainerQueuingLimitProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMaxQueueLength();
  }

  @Override
  public void setMaxQueueLength(int queueLength) {
    maybeInitBuilder();
    builder.setMaxQueueLength(queueLength);
  }
}
