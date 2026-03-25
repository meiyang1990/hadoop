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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;

/**
 * 资源管理器Web DAO类，封装应用执行类型请求信息，用于REST接口序列化返回
 */
@XmlRootElement(name = "ExecutionTypeRequest")
@XmlAccessorType(XmlAccessType.FIELD)
public class ExecutionTypeRequestInfo {
  @XmlElement(name = "executionType")
  private String executionType;
  @XmlElement(name = "enforceExecutionType")
  private boolean enforceExecutionType;

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public ExecutionTypeRequestInfo() {
  }

  /**
   * 从原始执行类型请求对象构造Web层信息对象
   * @param executionTypeRequest 原始执行类型请求
   */
  public ExecutionTypeRequestInfo(ExecutionTypeRequest executionTypeRequest) {
    executionType = executionTypeRequest.getExecutionType().name();
    enforceExecutionType = executionTypeRequest.getEnforceExecutionType();
  }

  /**
   * 获取执行类型枚举
   * @return 执行类型
   */
  public ExecutionType getExecutionType() {
    return ExecutionType.valueOf(executionType);
  }

  /**
   * 设置执行类型
   * @param executionType 执行类型枚举
   */
  public void setExecutionType(ExecutionType executionType) {
    this.executionType = executionType.name();
  }

  /**
   * 获取是否强制要求执行类型
   * @return true表示强制，false表示不强制
   */
  public boolean getEnforceExecutionType() {
    return enforceExecutionType;
  }

  /**
   * 设置是否强制要求执行类型
   * @param enforceExecutionType 是否强制
   */
  public void setEnforceExecutionType(boolean enforceExecutionType) {
    this.enforceExecutionType = enforceExecutionType;
  }
}