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
package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerActionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.ResourceLocalizationSpecPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;

/**
 * Localizer心跳响应Protobuf实现类，NodeManager响应本地化器心跳的协议消息实现
 * 基于Protobuf序列化，实现了LocalizerHeartbeatResponse接口
 */
public class LocalizerHeartbeatResponsePBImpl
        extends ProtoBase<LocalizerHeartbeatResponseProto>
        implements LocalizerHeartbeatResponse {

  // Protobuf默认实例
  LocalizerHeartbeatResponseProto proto =
    LocalizerHeartbeatResponseProto.getDefaultInstance();
  // Protobuf构建器
  LocalizerHeartbeatResponseProto.Builder builder = null;
  // 标记是否通过Protobuf实例构造
  boolean viaProto = false;

  // 缓存资源本地化规范列表
  private List<ResourceLocalizationSpec> resourceSpecs;

  /**
   * 空构造函数，初始化Protobuf构建器
   */
  public LocalizerHeartbeatResponsePBImpl() {
    builder = LocalizerHeartbeatResponseProto.newBuilder();
  }

  /**
   * 通过已有Protobuf实例构造响应对象
   * @param proto 已构造好的Protobuf响应对象
   */
  public LocalizerHeartbeatResponsePBImpl(
      LocalizerHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public LocalizerHeartbeatResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 合并本地缓存数据到Protobuf构建器
  private void mergeLocalToBuilder() {
    if (resourceSpecs != null) {
      addResourcesToProto();
    }
  }

  // 合并本地缓存数据到最终Protobuf实例
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Protobuf构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = LocalizerHeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public LocalizerAction getLocalizerAction() {
    LocalizerHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAction()) {
      return null;
    }
    return convertFromProtoFormat(p.getAction());
  }

  @Override
  public List<ResourceLocalizationSpec> getResourceSpecs() {
    initResources();
    return this.resourceSpecs;
  }

  @Override
  public void setLocalizerAction(LocalizerAction action) {
    maybeInitBuilder();
    if (action == null) {
      builder.clearAction();
      return;
    }
    builder.setAction(convertToProtoFormat(action));
  }

  @Override
  public void setResourceSpecs(List<ResourceLocalizationSpec> rsrcs) {
    maybeInitBuilder();
    if (rsrcs == null) {
      builder.clearResources();
      return;
    }
    this.resourceSpecs = rsrcs;
  }

  // 延迟初始化资源本地化规范列表，从Protobuf解析数据
  private void initResources() {
    if (this.resourceSpecs != null) {
      return;
    }
    LocalizerHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceLocalizationSpecProto> list = p.getResourcesList();
    this.resourceSpecs = new ArrayList<ResourceLocalizationSpec>();
    for (ResourceLocalizationSpecProto c : list) {
      this.resourceSpecs.add(convertFromProtoFormat(c));
    }
  }

  // 将本地缓存的资源规范写入Protobuf构建器
  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResources();
    if (this.resourceSpecs == null) 
      return;
    Iterable<ResourceLocalizationSpecProto> iterable =
        new Iterable<ResourceLocalizationSpecProto>() {
      @Override
      public Iterator<ResourceLocalizationSpecProto> iterator() {
        return new Iterator<ResourceLocalizationSpecProto>() {

          Iterator<ResourceLocalizationSpec> iter = resourceSpecs.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceLocalizationSpecProto next() {
            ResourceLocalizationSpec resource = iter.next();
            
            return ((ResourceLocalizationSpecPBImpl)resource).getProto();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllResources(iterable);
  }


  // Protobuf格式转换为API对象
  private ResourceLocalizationSpec convertFromProtoFormat(
      ResourceLocalizationSpecProto p) {
    return new ResourceLocalizationSpecPBImpl(p);
  }

  // API对象转换为Protobuf格式
  private LocalizerActionProto convertToProtoFormat(LocalizerAction a) {
    return LocalizerActionProto.valueOf(a.name());
  }

  // Protobuf格式转换为API对象
  private LocalizerAction convertFromProtoFormat(LocalizerActionProto a) {
    return LocalizerAction.valueOf(a.name());
  }
}