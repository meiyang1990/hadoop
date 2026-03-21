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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.RMDelegationTokenIdentifierDataProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;

/**
 * RM 委托令牌标识符持久化数据包装类，用于资源管理器恢复时存储和恢复委托令牌信息。
 * 封装了委托令牌标识符和更新日期，基于Protobuf实现序列化。
 */
public class RMDelegationTokenIdentifierData {
  // Protobuf Builder，用于构建和序列化持久化数据
  RMDelegationTokenIdentifierDataProto.Builder builder =
      RMDelegationTokenIdentifierDataProto.newBuilder();

  /**
   * 空构造方法，用于反序列化场景。
   */
  public RMDelegationTokenIdentifierData() {}

  /**
   * 构造方法，根据现有委托令牌标识符和更新日期构建持久化数据对象。
   * @param identifier 委托令牌标识符
   * @param renewdate 令牌更新日期
   */
  public RMDelegationTokenIdentifierData(
      YARNDelegationTokenIdentifier identifier, long renewdate) {
    builder.setTokenIdentifier(identifier.getProto());
    builder.setRenewDate(renewdate);
  }

  /**
   * 从输入流反序列化恢复对象状态。
   * @param in 输入流
   * @throws IOException 反序列化失败时抛出异常
   */
  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }

  /**
   * 将当前对象序列化为字节数组，用于持久化存储。
   * @return 序列化后的字节数组
   * @throws IOException 序列化失败时抛出异常
   */
  public byte[] toByteArray() throws IOException {
    return builder.build().toByteArray();
  }

  /**
   * 从存储数据中恢复并获取RM委托令牌标识符。
   * @return 恢复后的RM委托令牌标识符
   * @throws IOException 反序列化令牌失败时抛出异常
   */
  public RMDelegationTokenIdentifier getTokenIdentifier() throws IOException {
    ByteArrayInputStream in =
        new ByteArrayInputStream(builder.getTokenIdentifier().toByteArray());
    RMDelegationTokenIdentifier identifer = new RMDelegationTokenIdentifier();
    identifer.readFields(new DataInputStream(in));
    return identifer;
  }

  /**
   * 获取令牌更新日期。
   * @return 令牌更新日期时间戳
   */
  public long getRenewDate() {
    return builder.getRenewDate();
  }

  /**
   * 设置委托令牌标识符。
   * @param identifier 委托令牌标识符
   */
  public void setIdentifier(YARNDelegationTokenIdentifier identifier) {
    builder.setTokenIdentifier(identifier.getProto());
  }

  /**
   * 设置令牌更新日期。
   * @param renewDate 令牌更新日期时间戳
   */
  public void setRenewDate(long renewDate) {
    builder.setRenewDate(renewDate);
  }
}