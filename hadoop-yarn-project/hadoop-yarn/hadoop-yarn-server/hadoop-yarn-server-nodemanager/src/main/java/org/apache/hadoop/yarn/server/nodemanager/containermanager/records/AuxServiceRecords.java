// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.List;

/**
 * NodeManager 辅助服务记录集合，用于序列化和传输NodeManager上的辅助服务信息。
 * 主要用于Web UI等场景对外暴露辅助服务列表。
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuxServiceRecords {
  // 存储所有辅助服务信息的列表
  private List<AuxServiceRecord> services = new ArrayList<>();

  /**
   * 批量添加辅助服务记录到集合，支持Builder流式调用。
   * @param serviceList 待添加的辅助服务记录数组
   * @return 当前AuxServiceRecords实例，支持链式调用
   */
  public AuxServiceRecords serviceList(AuxServiceRecord... serviceList) {
    for (AuxServiceRecord service : serviceList) {
      this.services.add(service);
    }
    return this;
  }

  /**
   * 获取所有辅助服务记录列表。
   * @return 辅助服务记录列表
   */
  public List<AuxServiceRecord> getServices() {
    return services;
  }
}