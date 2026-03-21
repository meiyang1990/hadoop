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
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalResourceStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;

/**
 * LocalizerStatus的Protobuf实现，用于NodeManager与Localizer之间
 * 通信时序列化本地化状态信息
 */
public class LocalizerStatusPBImpl
    extends ProtoBase<LocalizerStatusProto> implements LocalizerStatus {

  // Protobuf默认实例
  LocalizerStatusProto proto =
    LocalizerStatusProto.getDefaultInstance();
  // Protobuf构建器
  LocalizerStatusProto.Builder builder = null;
  // 当前是否通过Proto模式持有数据
  boolean viaProto = false;

  // 缓存本地资源状态列表
  private List<LocalResourceStatus> resources = null;

  /**
   * 无参构造函数，初始化Builder
   */
  public LocalizerStatusPBImpl() {
    builder = LocalizerStatusProto.newBuilder();
  }

  /**
   * 基于已有Proto实例构造，复用现有Proto数据
   * @param proto 已有的LocalizerStatusProto实例
   */
  public LocalizerStatusPBImpl(LocalizerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public LocalizerStatusProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存数据合并到Builder中
  private void mergeLocalToBuilder() {
    if (this.resources != null) {
      addResourcesToProto();
    }
  }

  // 将本地缓存数据合并到最终Proto实例
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Builder，基于现有Proto构建
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = LocalizerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getLocalizerId() {
    LocalizerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLocalizerId()) {
      return null;
    }
    return (p.getLocalizerId());
  }

  @Override
  public List<LocalResourceStatus> getResources() {
    initResources();
    return this.resources;
  }

  @Override
  public void setLocalizerId(String localizerId) {
    maybeInitBuilder();
    if (localizerId == null) {
      builder.clearLocalizerId();
      return;
    }
    builder.setLocalizerId(localizerId);
  }

  // 延迟初始化资源列表，从Proto转换为Java对象
  private void initResources() {
    if (this.resources != null) {
      return;
    }
    LocalizerStatusProtoOrBuilder p = viaProto ? proto : builder;
    List<LocalResourceStatusProto> list = p.getResourcesList();
    this.resources = new ArrayList<LocalResourceStatus>();

    for (LocalResourceStatusProto c : list) {
      this.resources.add(convertFromProtoFormat(c));
    }
  }

  // 将Java对象格式的资源列表转换回Proto格式写入Builder
  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResources();
    if (this.resources == null) 
      return;
    Iterable<LocalResourceStatusProto> iterable =
        new Iterable<LocalResourceStatusProto>() {
      @Override
      public Iterator<LocalResourceStatusProto> iterator() {
        return new Iterator<LocalResourceStatusProto>() {

          Iterator<LocalResourceStatus> iter = resources.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public LocalResourceStatusProto next() {
            return convertToProtoFormat(iter.next());
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

  @Override
  public void addAllResources(List<LocalResourceStatus> resources) {
    if (resources == null)
      return;
    initResources();
    this.resources.addAll(resources);
  }

  @Override
  public LocalResourceStatus getResourceStatus(int index) {
    initResources();
    return this.resources.get(index);
  }

  @Override
  public void addResourceStatus(LocalResourceStatus resource) {
    initResources();
    this.resources.add(resource);
  }

  @Override
  public void removeResource(int index) {
    initResources();
    this.resources.remove(index);
  }

  @Override
  public void clearResources() {
    initResources();
    this.resources.clear();
  }

  /**
   * 将Proto格式转换为LocalResourceStatus业务对象
   * @param p Proto格式资源状态
   * @return 业务对象实例
   */
  private LocalResourceStatus
      convertFromProtoFormat(LocalResourceStatusProto p) {
    return new LocalResourceStatusPBImpl(p);
  }

  /**
   * 将LocalResourceStatus业务对象转换为Proto格式
   * @param s 业务对象实例
   * @return Proto格式资源状态
   */
  private LocalResourceStatusProto convertToProtoFormat(LocalResourceStatus s) {
    return ((LocalResourceStatusPBImpl)s).getProto();
  }

}