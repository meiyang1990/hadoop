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

package org.apache.hadoop.yarn.server.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProtoOrBuilder;

import org.apache.hadoop.yarn.server.records.Version;

/**
 * Version 接口的 Protobuf 实现类，用于版本信息的序列化与反序列化，
 * 在YARN服务间通信中承载版本兼容性检查信息。
 */
public class VersionPBImpl extends Version {

  // 底层存储的Protobuf版本对象实例
  VersionProto proto = VersionProto.getDefaultInstance();
  // Protobuf构建器，用于构建修改版本对象
  VersionProto.Builder builder = null;
  // 标记当前是否直接使用proto存储数据，false表示正在通过builder修改
  boolean viaProto = false;

  /**
   * 无参构造，初始化空的版本构建器。
   */
  public VersionPBImpl() {
    builder = VersionProto.newBuilder();
  }

  /**
   * 通过已有Protobuf对象构造版本实例。
   * @param proto 已有的版本Protobuf对象
   */
  public VersionPBImpl(VersionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前版本对应的Protobuf对象，懒构建最终实例。
   * @return 构建完成的版本Protobuf对象
   */
  public VersionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 确保builder已初始化，准备接收修改。
   * 如果当前使用proto存储，则将proto数据拷贝到builder中。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = VersionProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getMajorVersion() {
    VersionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMajorVersion();
  }

  @Override
  public void setMajorVersion(int major) {
    maybeInitBuilder();
    builder.setMajorVersion(major);
  }

  @Override
  public int getMinorVersion() {
    VersionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMinorVersion();
  }

  @Override
  public void setMinorVersion(int minor) {
    maybeInitBuilder();
    builder.setMinorVersion(minor);
  }
}