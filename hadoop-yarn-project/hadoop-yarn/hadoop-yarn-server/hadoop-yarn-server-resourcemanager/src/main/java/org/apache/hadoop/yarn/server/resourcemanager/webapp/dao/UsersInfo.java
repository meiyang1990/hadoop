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

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;

/**
 * YARN ResourceManager Web UI 多用户信息数据访问对象，封装所有调度用户信息列表，用于REST API返回序列化。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class UsersInfo {
  // 序列化后的XML元素名设为user，存储所有用户信息
  @XmlElement(name="user")
  protected ArrayList<UserInfo> usersList = new ArrayList<UserInfo>();

  /**
   * 无参构造器，供JAXB序列化使用。
   */
  public UsersInfo() {
  }

  /**
   * 构造器，用给定的用户信息列表初始化对象。
   * @param usersList 容量调度用户信息列表
   */
  public UsersInfo(ArrayList<UserInfo> usersList) {
    this.usersList = usersList;
  }

  /**
   * 获取所有用户信息列表。
   * @return 容量调度用户信息列表
   */
  public ArrayList<UserInfo> getUsersList() {
    return usersList;
  }
}