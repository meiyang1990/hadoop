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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner;

import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeState;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;

import java.util.HashMap;
import java.util.Map;

/**
 * CSI存储卷批量预配结果聚合容器，存储多个卷的预配结果并提供整体状态查询能力
 */
public class VolumeProvisioningResults {

  private Map<VolumeId, VolumeProvisioningResult> resultMap;

  /**
   * 构造批量预配结果对象，初始化结果存储
   */
  public VolumeProvisioningResults() {
    this.resultMap = new HashMap<>();
  }

  /**
   * 检查所有卷的预配是否全部成功
   * @return 所有卷预配成功返回true，否则返回false
   */
  public boolean isSuccess() {
    return !resultMap.isEmpty() && resultMap.values().stream()
        .allMatch(subResult -> subResult.isSuccess());
  }

  /**
   * 生成预配结果的JSON格式简要信息，包含总卷数和失败卷状态
   * @return JSON格式的结果摘要字符串
   */
  public String getBriefMessage() {
    // 创建顶层JSON对象
    JsonObject obj = new JsonObject();
    // 添加总卷数字段
    obj.addProperty("TotalVolumes", resultMap.size());

    // 创建存储失败卷信息的JSON对象
    JsonObject failed = new JsonObject();
    // 遍历所有预配结果，收集失败卷信息
    for (VolumeProvisioningResult result : resultMap.values()) {
      if (!result.isSuccess()) {
        failed.addProperty(result.getVolumeId().toString(),
            result.getVolumeState().name());
      }
    }
    // 将失败卷信息添加到顶层对象
    obj.add("failedVolumesStates", failed);
    // 返回JSON字符串
    return obj.toString();
  }

  /**
   * 单个CSI存储卷的预配结果，存储卷ID、当前状态和预配是否成功
   */
  static class VolumeProvisioningResult {

    private VolumeId volumeId;
    private VolumeState volumeState;
    private boolean success;

    /**
     * 构造单个卷预配结果
     * @param volumeId 卷ID
     * @param state 卷当前状态
     */
    VolumeProvisioningResult(VolumeId volumeId, VolumeState state) {
      this.volumeId = volumeId;
      this.volumeState = state;
      // 只有当卷状态为NODE_READY时才认为预配成功
      this.success = state == VolumeState.NODE_READY;
    }

    /**
     * 获取单个卷预配是否成功
     * @return 预配成功返回true，否则返回false
     */
    public boolean isSuccess() {
      return this.success;
    }

    /**
     * 获取当前卷ID
     * @return 卷ID对象
     */
    public VolumeId getVolumeId() {
      return this.volumeId;
    }

    /**
     * 获取当前卷状态
     * @return 卷状态枚举
     */
    public VolumeState getVolumeState() {
      return this.volumeState;
    }
  }

  /**
   * 添加单个卷的预配结果到批量结果中
   * @param volumeId 卷ID
   * @param state 卷当前状态
   */
  public void addResult(VolumeId volumeId, VolumeState state) {
    this.resultMap.put(volumeId,
        new VolumeProvisioningResult(volumeId, state));
  }
}