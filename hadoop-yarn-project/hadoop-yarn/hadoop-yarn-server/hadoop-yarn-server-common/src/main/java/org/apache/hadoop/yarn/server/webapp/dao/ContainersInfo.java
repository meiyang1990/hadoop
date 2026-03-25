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
package org.apache.hadoop.yarn.server.webapp.dao;

import java.util.ArrayList;
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * YARN Web REST API 容器列表信息数据访问对象，用于序列化/反序列化所有容器信息
 */
@Public
@Evolving
@XmlRootElement(name = "containers")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainersInfo {

  // 存储单个容器信息的列表
  protected ArrayList<ContainerInfo> container = new ArrayList<ContainerInfo>();

  /**
   * 默认构造函数，供JAXB序列化使用
   */
  public ContainersInfo() {
    // JAXB needs this
  }

  /**
   * 添加单个容器信息到列表
   * @param containerInfo 单个容器信息对象
   */
  public void add(ContainerInfo containerInfo) {
    container.add(containerInfo);
  }

  /**
   * 获取所有容器信息列表
   * @return 容器信息对象列表
   */
  public ArrayList<ContainerInfo> getContainers() {
    return container;
  }

  /**
   * 批量添加多个容器信息到列表
   * @param containersInfo 多个容器信息集合
   */
  public void addAll(Collection<ContainerInfo> containersInfo) {
    container.addAll(containersInfo);
  }
}