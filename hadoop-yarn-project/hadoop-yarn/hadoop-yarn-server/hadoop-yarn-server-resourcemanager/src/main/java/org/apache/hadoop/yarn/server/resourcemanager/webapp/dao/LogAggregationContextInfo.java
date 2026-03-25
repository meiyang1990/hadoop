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

/**
 * 日志聚合上下文信息数据传输对象，用于REST API接收客户端提交的日志聚合配置，
 * 用于构建容器启动上下文，最终作为应用提交上下文的一部分。
 *
 */
@XmlRootElement(name = "log-aggregation-context")
@XmlAccessorType(XmlAccessType.FIELD)
public class LogAggregationContextInfo {

  @XmlElement(name = "log-include-pattern")
  String logIncludePattern;

  @XmlElement(name = "log-exclude-pattern")
  String logExcludePattern;

  @XmlElement(name = "rolled-log-include-pattern")
  String rolledLogsIncludePattern;

  @XmlElement(name = "rolled-log-exclude-pattern")
  String rolledLogsExcludePattern;

  @XmlElement(name = "log-aggregation-policy-class-name")
  String policyClassName;

  @XmlElement(name = "log-aggregation-policy-parameters")
  String policyParameters;

  public LogAggregationContextInfo() {
  }

  /**
   * 获取需要聚合包含的日志匹配模式。
   * @return 日志包含匹配模式字符串
   */
  public String getIncludePattern() {
    return this.logIncludePattern;
  }

  /**
   * 设置需要聚合包含的日志匹配模式。
   * @param includePattern 日志包含匹配模式字符串
   */
  public void setIncludePattern(String includePattern) {
    this.logIncludePattern = includePattern;
  }

  /**
   * 获取需要聚合排除的日志匹配模式。
   * @return 日志排除匹配模式字符串
   */
  public String getExcludePattern() {
    return this.logExcludePattern;
  }

  /**
   * 设置需要聚合排除的日志匹配模式。
   * @param excludePattern 日志排除匹配模式字符串
   */
  public void setExcludePattern(String excludePattern) {
    this.logExcludePattern = excludePattern;
  }

  /**
   * 获取滚动日志需要聚合包含的匹配模式。
   * @return 滚动日志包含匹配模式字符串
   */
  public String getRolledLogsIncludePattern() {
    return this.rolledLogsIncludePattern;
  }

  /**
   * 设置滚动日志需要聚合包含的匹配模式。
   * @param rolledLogsIncludePattern 滚动日志包含匹配模式字符串
   */
  public void setRolledLogsIncludePattern(
      String rolledLogsIncludePattern) {
    this.rolledLogsIncludePattern = rolledLogsIncludePattern;
  }

  /**
   * 获取滚动日志需要聚合排除的匹配模式。
   * @return 滚动日志排除匹配模式字符串
   */
  public String getRolledLogsExcludePattern() {
    return this.rolledLogsExcludePattern;
  }

  /**
   * 设置滚动日志需要聚合排除的匹配模式。
   * @param rolledLogsExcludePattern 滚动日志排除匹配模式字符串
   */
  public void setRolledLogsExcludePattern(
      String rolledLogsExcludePattern) {
    this.rolledLogsExcludePattern = rolledLogsExcludePattern;
  }

  /**
   * 获取日志聚合策略实现类的全类名。
   * @return 日志聚合策略类全类名字符串
   */
  public String getLogAggregationPolicyClassName() {
    return this.policyClassName;
  }

  /**
   * 设置日志聚合策略实现类的全类名。
   * @param className 日志聚合策略类全类名字符串
   */
  public void setLogAggregationPolicyClassName(
      String className) {
    this.policyClassName = className;
  }

  /**
   * 获取日志聚合策略的初始化参数字符串。
   * @return 日志聚合策略初始化参数
   */
  public String getLogAggregationPolicyParameters() {
    return this.policyParameters;
  }

  /**
   * 设置日志聚合策略的初始化参数字符串。
   * @param parameters 日志聚合策略初始化参数
   */
  public void setLogAggregationPolicyParameters(
      String parameters) {
    this.policyParameters = parameters;
  }
}