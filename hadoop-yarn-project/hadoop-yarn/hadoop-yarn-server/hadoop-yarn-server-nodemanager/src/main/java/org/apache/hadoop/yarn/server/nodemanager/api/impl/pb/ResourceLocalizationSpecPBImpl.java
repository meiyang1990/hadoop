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
package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.URLPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;

/**
 * 资源本地化规格的Protobuf实现，用于NodeManager资源本地化RPC通信的数据结构封装
 */
public class ResourceLocalizationSpecPBImpl extends
    ProtoBase<ResourceLocalizationSpecProto> implements
    ResourceLocalizationSpec {

  // Protobuf协议对象，用于序列化/反序列化
  private ResourceLocalizationSpecProto proto = ResourceLocalizationSpecProto
    .getDefaultInstance();
  // Protobuf构建器，用于构建新对象
  private ResourceLocalizationSpecProto.Builder builder = null;
  // 当前是否直接使用proto对象，false表示通过builder构建
  private boolean viaProto;
  // 待本地化的资源对象
  private LocalResource resource = null;
  // 本地化目标目录URL
  private URL destinationDirectory = null;

  /**
   * 构造空的资源本地化规格对象，初始化构建器
   */
  public ResourceLocalizationSpecPBImpl() {
    builder = ResourceLocalizationSpecProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造资源本地化规格
   * @param proto 已有的Protobuf协议对象
   */
  public ResourceLocalizationSpecPBImpl(ResourceLocalizationSpecProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public LocalResource getResource() {
    // 根据当前状态选择proto或builder
    ResourceLocalizationSpecProtoOrBuilder p = viaProto ? proto : builder;
    // 已有缓存对象直接返回
    if (resource != null) {
      return resource;
    }
    // proto中不存在该字段返回null
    if (!p.hasResource()) {
      return null;
    }
    // 从Protobuf构造LocalResource对象并缓存
    resource = new LocalResourcePBImpl(p.getResource());
    return resource;
  }

  @Override
  public void setResource(LocalResource rsrc) {
    maybeInitBuilder();
    // 缓存设置的资源对象
    resource = rsrc;
  }

  @Override
  public URL getDestinationDirectory() {
    // 根据当前状态选择proto或builder
    ResourceLocalizationSpecProtoOrBuilder p = viaProto ? proto : builder;
    // 已有缓存对象直接返回
    if (destinationDirectory != null) {
      return destinationDirectory;
    }
    // proto中不存在该字段返回null
    if (!p.hasDestinationDirectory()) {
      return null;
    }
    // 从Protobuf构造URL对象并缓存
    destinationDirectory = new URLPBImpl(p.getDestinationDirectory());
    return destinationDirectory;
  }

  @Override
  public void setDestinationDirectory(URL destinationDirectory) {
    maybeInitBuilder();
    // 缓存设置的目标目录
    this.destinationDirectory = destinationDirectory;
  }

  @Override
  public ResourceLocalizationSpecProto getProto() {
    // 将本地缓存对象合并到构建器
    mergeLocalToBuilder();
    // 构建并缓存最终proto对象，标记后续使用proto
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 初始化构建器，确保可以修改对象，切换到builder模式
   */
  private synchronized void maybeInitBuilder() {
    if (builder == null || viaProto) {
      // 基于已有proto创建新的构建器
      builder = ResourceLocalizationSpecProto.newBuilder(proto);
    }
    // 切换到builder模式
    viaProto = false;
  }

  /**
   * 将本地缓存的Java对象合并到Protobuf构建器中，确保数据一致性
   */
  private void mergeLocalToBuilder() {
    ResourceLocalizationSpecProtoOrBuilder l = viaProto ? proto : builder;
    // 如果本地资源已修改，更新到builder中
    if (this.resource != null
        && !(l.getResource()
          .equals(((LocalResourcePBImpl) resource).getProto()))) {
      maybeInitBuilder();
      builder.setResource(((LocalResourcePBImpl) resource).getProto());
    }
    // 如果目标目录已修改，更新到builder中
    if (this.destinationDirectory != null
        && !(l.getDestinationDirectory()
          .equals(((URLPBImpl) destinationDirectory).getProto()))) {
      maybeInitBuilder();
      builder.setDestinationDirectory(((URLPBImpl) destinationDirectory)
        .getProto());
    }
  }
}