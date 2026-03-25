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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 上报新日志聚合收集器信息响应的Protobuf实现类
 */
@Private
@Unstable
public class ReportNewCollectorInfoResponsePBImpl extends
    ReportNewCollectorInfoResponse {

  // Protobuf协议对象，通过已有proto实例构建时使用
  private ReportNewCollectorInfoResponseProto proto =
      ReportNewCollectorInfoResponseProto.getDefaultInstance();

  // Protobuf构建器，新建对象时使用
  private ReportNewCollectorInfoResponseProto.Builder builder = null;

  // 标识当前是否通过proto实例而非构建器持有数据
  private boolean viaProto = false;

  /**
   * 构造空响应对象，使用构建器初始化
   */
  public ReportNewCollectorInfoResponsePBImpl() {
    builder = ReportNewCollectorInfoResponseProto.newBuilder();
  }

  /**
   * 基于已有proto实例构造响应对象
   * @param proto 已构建完成的proto响应实例
   */
  public ReportNewCollectorInfoResponsePBImpl(
      ReportNewCollectorInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的proto实例，按需构建
   * @return 构建完成的proto响应实例
   */
  public ReportNewCollectorInfoResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

}