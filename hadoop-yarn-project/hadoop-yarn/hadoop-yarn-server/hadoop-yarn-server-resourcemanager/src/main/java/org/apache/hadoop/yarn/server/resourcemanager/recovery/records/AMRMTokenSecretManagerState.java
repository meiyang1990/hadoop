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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;

/**
 * AMRMTokenSecretManager的持久化状态存储容器，保存AMRM令牌密钥管理器需要持久化恢复的所有状态数据
 * 用于ResourceManager故障恢复场景，重启后恢复AMRM令牌密钥状态
 */
@Public
@Unstable
public abstract class AMRMTokenSecretManagerState {
  
  /**
   * 根据当前和下一个主密钥创建AMRMTokenSecretManager状态实例
   * @param currentMasterKey 当前生效的主密钥
   * @param nextMasterKey 下一个将生效的主密钥
   * @return 新建的状态实例
   */
  public static AMRMTokenSecretManagerState newInstance(
      MasterKey currentMasterKey, MasterKey nextMasterKey) {
    AMRMTokenSecretManagerState data =
        Records.newRecord(AMRMTokenSecretManagerState.class);
    data.setCurrentMasterKey(currentMasterKey);
    data.setNextMasterKey(nextMasterKey);
    return data;
  }

  /**
   * 根据已有状态拷贝创建新的AMRMTokenSecretManager状态实例
   * @param state 源状态对象
   * @return 拷贝后的新状态实例
   */
  public static AMRMTokenSecretManagerState newInstance(
      AMRMTokenSecretManagerState state) {
    AMRMTokenSecretManagerState data =
        Records.newRecord(AMRMTokenSecretManagerState.class);
    data.setCurrentMasterKey(state.getCurrentMasterKey());
    data.setNextMasterKey(state.getNextMasterKey());
    return data;
  }

  /**
   * 获取AMRMTokenSecretManager当前生效的主密钥
   * @return 当前主密钥
   */
  @Public
  @Unstable
  public abstract MasterKey getCurrentMasterKey();

  @Public
  @Unstable
  public abstract void setCurrentMasterKey(MasterKey currentMasterKey);

  /**
   * 获取AMRMTokenSecretManager即将生效的下一个主密钥
   * @return 下一个主密钥
   */
  @Public
  @Unstable
  public abstract MasterKey getNextMasterKey();

  @Public
  @Unstable
  public abstract void setNextMasterKey(MasterKey nextMasterKey);

  /**
   * 将当前状态转换为Protobuf序列化对象，用于持久化存储
   * @return Protobuf格式的状态对象
   */
  public abstract AMRMTokenSecretManagerStateProto getProto();
}