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

import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;

/**
 * 应用超时信息数据访问对象，用于在ResourceManager Web UI中展示应用超时信息
 * DAO object to display Application timeout information.
 */
@XmlRootElement(name = "timeout")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppTimeoutInfo {

  @XmlElement(name = "type")
  // 超时类型（如生命周期超时等）
  private ApplicationTimeoutType timeoutType;

  @XmlElement(name = "expiryTime")
  // 超时过期时间字符串
  private String expiryTime;

  @XmlElement(name = "remainingTimeInSeconds")
  // 剩余超时时间，单位：秒
  private long remainingTimeInSec;

  /**
   * 默认构造函数，初始化无限制超时配置
   */
  public AppTimeoutInfo() {
    expiryTime = "UNLIMITED";
    remainingTimeInSec = -1;
  }

  /**
   * 根据应用超时记录构造AppTimeoutInfo对象
   * @param applicationTimeout 应用超时记录实体
   */
  public AppTimeoutInfo(ApplicationTimeout applicationTimeout) {
    this.expiryTime = applicationTimeout.getExpiryTime();
    this.remainingTimeInSec = applicationTimeout.getRemainingTime();
    this.timeoutType = applicationTimeout.getTimeoutType();
  }

  // 获取超时类型
  public ApplicationTimeoutType getTimeoutType() {
    return timeoutType;
  }

  // 获取过期时间
  public String getExpireTime() {
    return expiryTime;
  }

  // 获取剩余超时秒数
  public long getRemainingTimeInSec() {
    return remainingTimeInSec;
  }

  // 设置超时类型
  public void setTimeoutType(ApplicationTimeoutType type) {
    this.timeoutType = type;
  }

  // 设置过期时间
  public void setExpiryTime(String expiryTime) {
    this.expiryTime = expiryTime;
  }

  // 设置剩余超时秒数
  public void setRemainingTime(long remainingTime) {
    this.remainingTimeInSec = remainingTime;
  }
}