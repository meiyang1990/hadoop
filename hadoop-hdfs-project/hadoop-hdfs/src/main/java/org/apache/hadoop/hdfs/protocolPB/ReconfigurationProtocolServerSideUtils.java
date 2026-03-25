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
package org.apache.hadoop.hdfs.protocolPB;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusConfigChangeProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;

/**
 * HDFS 重配置协议服务端工具类，负责将Java内存中的重配置状态数据转换为Protobuf格式响应，
 * 供RPC服务返回给客户端，封装了重配置协议的公共序列化逻辑。
 */
public final class ReconfigurationProtocolServerSideUtils {
  /**
   * 工具类禁止实例化，私有构造方法
   */
  private ReconfigurationProtocolServerSideUtils() {
  }

  /**
   * 将可重配置属性列表转换为Protobuf响应对象，用于响应客户端查询可重配置属性请求
   * @param reconfigurableProperties 服务端支持动态重配置的属性名称列表
   * @return 序列化后的Protobuf响应对象，可直接通过RPC返回
   */
  public static ListReconfigurablePropertiesResponseProto
      listReconfigurableProperties(
          List<String> reconfigurableProperties) {
    ListReconfigurablePropertiesResponseProto.Builder builder =
        ListReconfigurablePropertiesResponseProto.newBuilder();
    builder.addAllName(reconfigurableProperties);
    return builder.build();
  }

  /**
   * 将重配置任务状态转换为Protobuf响应对象，用于响应客户端查询重配置状态请求
   * @param status 重配置任务的当前状态对象，包含启动时间、结束时间和各属性修改结果
   * @return 序列化后的Protobuf响应对象，可直接通过RPC返回
   */
  public static GetReconfigurationStatusResponseProto getReconfigurationStatus(
      ReconfigurationTaskStatus status) {
    GetReconfigurationStatusResponseProto.Builder builder =
        GetReconfigurationStatusResponseProto.newBuilder();

    // 设置任务启动时间
    builder.setStartTime(status.getStartTime());
    // 如果任务已经结束，填充结束时间和修改结果
    if (status.stopped()) {
      builder.setEndTime(status.getEndTime());
      assert status.getStatus() != null;
      // 遍历每个属性的修改结果，逐个转换为Protobuf格式
      for (Map.Entry<PropertyChange, Optional<String>> result : status
          .getStatus().entrySet()) {
        GetReconfigurationStatusConfigChangeProto.Builder changeBuilder =
            GetReconfigurationStatusConfigChangeProto.newBuilder();
        PropertyChange change = result.getKey();
        // 设置属性名称
        changeBuilder.setName(change.prop);
        // 设置旧值，空值转换为空字符串
        changeBuilder.setOldValue(change.oldVal != null ? change.oldVal : "");
        // 设置新值，非空才填充
        if (change.newVal != null) {
          changeBuilder.setNewValue(change.newVal);
        }
        // 如果修改出错，填充错误信息（包含完整堆栈）
        if (result.getValue().isPresent()) {
          // 获取完整异常堆栈信息作为错误信息
          changeBuilder.setErrorMessage(result.getValue().get());
        }
        // 将当前属性修改结果添加到响应
        builder.addChanges(changeBuilder);
      }
    }
    return builder.build();
  }
}