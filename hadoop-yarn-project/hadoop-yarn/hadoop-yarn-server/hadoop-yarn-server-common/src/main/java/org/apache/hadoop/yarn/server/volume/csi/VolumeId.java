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
package org.apache.hadoop.yarn.server.volume.csi;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.util.StringUtils;

/**
 * CSI存储卷的唯一标识类。ID可能来自底层存储系统，也可由YARN生成，
 * YARN依赖该ID识别存储卷并管理其生命周期状态。
 */
public class VolumeId {

  private final String volumeId;

  /**
   * 构造方法，基于指定ID字符串创建存储卷标识。
   * @param volumeId 存储卷唯一ID字符串
   */
  public VolumeId(String volumeId) {
    this.volumeId = volumeId;
  }

  /**
   * 获取存储卷ID字符串。
   * @return 存储卷唯一ID
   */
  public String getId() {
    return this.volumeId;
  }

  @Override
  public String toString() {
    return this.volumeId;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof VolumeId)) {
      return false;
    }
    // 不区分大小写比较ID字符串
    return StringUtils.equalsIgnoreCase(volumeId,
        ((VolumeId) obj).getId());
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hc = new HashCodeBuilder();
    hc.append(volumeId);
    return hc.toHashCode();
  }
}