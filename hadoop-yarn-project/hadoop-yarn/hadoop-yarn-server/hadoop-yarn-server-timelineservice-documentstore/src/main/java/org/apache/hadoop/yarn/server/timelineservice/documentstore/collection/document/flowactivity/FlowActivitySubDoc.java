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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity;

/**
 * 流活动嵌套文档，用于存储单个YARN流运行的元数据信息，嵌入在流活动主文档中。
 */
public class FlowActivitySubDoc {
  private String flowName;
  private String flowVersion;
  private long flowRunId;

  /**
   * 无参构造器，用于反序列化。
   */
  public FlowActivitySubDoc() {
  }

  /**
   * 全参数构造器，创建流活动嵌套文档实例。
   * @param flowName 流名称
   * @param flowVersion 流版本
   * @param flowRunId 流运行ID
   */
  public FlowActivitySubDoc(String flowName, String flowVersion,
      long flowRunId) {
    this.flowName = flowName;
    this.flowVersion = flowVersion;
    this.flowRunId = flowRunId;
  }

  /**
   * 获取流名称。
   * @return 流名称
   */
  public String getFlowName() {
    return flowName;
  }

  /**
   * 获取流版本。
   * @return 流版本
   */
  public String getFlowVersion() {
    return flowVersion;
  }

  /**
   * 获取流运行ID。
   * @return 流运行ID
   */
  public long getFlowRunId() {
    return flowRunId;
  }

  @Override
  public int hashCode() {
    int result = flowVersion.hashCode();
    result = (int) (31 * result + flowRunId);
    return result;
  }

  // Only check if type and id are equal
  @Override
  public boolean equals(Object o) {
    // 同一对象直接相等
    if (this == o) {
      return true;
    }
    // 类型不匹配直接不相等
    if (!(o instanceof FlowActivitySubDoc)) {
      return false;
    }
    FlowActivitySubDoc m = (FlowActivitySubDoc) o;
    // 忽略大小写比较版本是否相等
    if (!flowVersion.equalsIgnoreCase(m.getFlowVersion())) {
      return false;
    }
    // 比较运行ID是否相等
    return flowRunId == m.getFlowRunId();
  }
}