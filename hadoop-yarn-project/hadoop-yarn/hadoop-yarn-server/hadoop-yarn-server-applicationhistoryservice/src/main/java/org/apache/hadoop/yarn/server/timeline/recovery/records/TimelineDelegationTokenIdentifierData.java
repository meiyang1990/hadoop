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

package org.apache.hadoop.yarn.server.timeline.recovery.records;

import org.apache.hadoop.yarn.proto.YarnServerTimelineServerRecoveryProtos.TimelineDelegationTokenIdentifierDataProto;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Timeline服务令牌标识符持久化存储数据封装类。
 * 用于在恢复服务中序列化存储时间线委托令牌标识符及其更新时间。
 */
public class TimelineDelegationTokenIdentifierData {
  // Protobuf构建器，用于序列化和反序列化数据
  TimelineDelegationTokenIdentifierDataProto.Builder builder =
      TimelineDelegationTokenIdentifierDataProto.newBuilder();

  /**
   * 构造空的令牌数据对象。
   */
  public TimelineDelegationTokenIdentifierData() {
  }

  /**
   * 构造令牌数据对象，根据传入的令牌标识符和更新时间初始化。
   * @param identifier 时间线委托令牌标识符
   * @param renewdate 令牌更新时间戳
   */
  public TimelineDelegationTokenIdentifierData(
      TimelineDelegationTokenIdentifier identifier, long renewdate) {
    builder.setTokenIdentifier(identifier.getProto());
    builder.setRenewDate(renewdate);
  }

  /**
   * 从输入流中反序列化读取数据。
   * @param in 输入流
   * @throws IOException 反序列化异常
   */
  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }

  /**
   * 将当前数据序列化为字节数组。
   * @return 序列化后的字节数组
   * @throws IOException 序列化异常
   */
  public byte[] toByteArray() throws IOException {
    return builder.build().toByteArray();
  }

  /**
   * 从存储数据中解析恢复出时间线委托令牌标识符。
   * @return 恢复后的令牌标识符对象
   * @throws IOException 反序列化解析异常
   */
  public TimelineDelegationTokenIdentifier getTokenIdentifier()
      throws IOException {
    ByteArrayInputStream in =
        new ByteArrayInputStream(builder.getTokenIdentifier().toByteArray());
    TimelineDelegationTokenIdentifier identifer =
        new TimelineDelegationTokenIdentifier();
    identifer.readFields(new DataInputStream(in));
    return identifer;
  }

  /**
   * 获取令牌最近更新时间戳。
   * @return 更新时间戳（毫秒）
   */
  public long getRenewDate() {
    return builder.getRenewDate();
  }
}