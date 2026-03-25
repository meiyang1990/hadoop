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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN ResourceManager 保活重启功能中的纪元信息记录类。
 * 每次RM重启都会递增纪元编号，用于保证全局ContainerId的唯一性。
 * 在RM重启后，新生成的ContainerId会基于新的纪元编号，不会和重启前的ContainerId重复。
 */
@Private
@Unstable
public abstract class Epoch {

  /**
   * 创建一个新的Epoch实例，设置指定的纪元序列号。
   * @param sequenceNumber 纪元序列号
   * @return 初始化完成的Epoch实例
   */
  public static Epoch newInstance(long sequenceNumber) {
    Epoch epoch = Records.newRecord(Epoch.class);
    epoch.setEpoch(sequenceNumber);
    return epoch;
  }

  /**
   * 获取当前纪元序列号。
   * @return 纪元序列号
   */
  public abstract long getEpoch();

  /**
   * 设置纪元序列号。
   * @param sequenceNumber 要设置的纪元序列号
   */
  public abstract void setEpoch(long sequenceNumber);

  /**
   * 获取当前Epoch的ProtoBuf序列化对象。
   * @return ProtoBuf格式的Epoch对象
   */
  public abstract EpochProto getProto();

  @Override
  public String toString() {
    return String.valueOf(getEpoch());
  }

  @Override
  public int hashCode() {
    return (int) (getEpoch() ^ (getEpoch() >>> 32));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Epoch other = (Epoch) obj;
    if (this.getEpoch() == other.getEpoch()) {
      return true;
    } else {
      return false;
    }
  }
}