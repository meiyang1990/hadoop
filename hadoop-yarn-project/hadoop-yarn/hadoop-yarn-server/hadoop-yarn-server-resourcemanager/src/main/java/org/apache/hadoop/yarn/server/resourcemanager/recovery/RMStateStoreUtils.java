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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * RM状态存储工具类，为RMStateStore及其子类提供通用工具方法。
 */
@Private
@Unstable
public class RMStateStoreUtils {

  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(RMStateStoreUtils.class);

  /**
   * 从输入流读取RM代理令牌数据，兼容当前Protobuf格式和旧版非Protobuf格式，
   * 用于恢复RM状态时读取持久化存储的代理令牌信息。
   *
   * @param fsIn 包含RM代理令牌数据的输入流
   * @return 读取完成的RM代理令牌数据对象
   * @throws IOException 读取过程中发生I/O错误
   */
  public static RMDelegationTokenIdentifierData
      readRMDelegationTokenIdentifierData(DataInputStream fsIn)
      throws IOException {
    RMDelegationTokenIdentifierData identifierData =
        new RMDelegationTokenIdentifierData();
    try {
      // 尝试按新版Protobuf格式读取
      identifierData.readFields(fsIn);
    } catch (InvalidProtocolBufferException e) {
      // Protobuf解析失败说明是旧版格式，回退到旧格式读取逻辑
      LOG.warn("Recovering old formatted token");
      // 重置输入流到起始位置，重新读取
      fsIn.reset();
      YARNDelegationTokenIdentifier identifier =
          new RMDelegationTokenIdentifier();
      // 按旧版格式读取令牌标识信息
      identifier.readFieldsInOldFormat(fsIn);
      // 设置读取到的标识信息
      identifierData.setIdentifier(identifier);
      // 读取并设置令牌更新时间
      identifierData.setRenewDate(fsIn.readLong());
    }
    return identifierData;
  }
}