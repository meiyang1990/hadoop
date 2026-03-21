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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN RM Web UI 资源信息数据访问对象，封装集群/节点/应用的资源信息，用于XML/JSON序列化输出
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ResourceInfo {

  @XmlElement
  long memory;
  @XmlElement
  int vCores;
  @XmlElement
  ResourceInformationsInfo resourceInformations =
      new ResourceInformationsInfo();

  // 内部持有原始资源对象
  private Resource resources;

  /**
   * JAXB反序列化需要的空构造函数
   */
  public ResourceInfo() {
  }

  /**
   * 从Resource对象构造ResourceInfo
   * @param res YARN原始资源对象
   */
  public ResourceInfo(Resource res) {
    if (res != null) {
      memory = res.getMemorySize();
      vCores = res.getVirtualCores();
      resources = Resources.clone(res);
      resourceInformations.addAll(res.getAllResourcesListCopy());
    }
  }

  /**
   * 获取内存容量（单位：MB），延迟初始化内部Resource对象
   * @return 内存大小
   */
  public long getMemorySize() {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    return resources.getMemorySize();
  }

  /**
   * 获取虚拟CPU核心数，延迟初始化内部Resource对象
   * @return CPU核心数
   */
  public int getvCores() {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    return resources.getVirtualCores();
  }

  @Override
  public String toString() {
    return getResource().toString();
  }

  /**
   * 转换为格式化的资源字符串输出
   * @return 格式化后的资源信息字符串
   */
  public String toFormattedString() {
    return getResource().toFormattedString();
  }

  /**
   * 设置内存容量，延迟初始化内部Resource对象
   * @param memory 内存大小（单位：MB）
   */
  public void setMemory(int memory) {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    this.memory = memory;
    resources.setMemorySize(memory);
  }

  /**
   * 设置虚拟CPU核心数，延迟初始化内部Resource对象
   * @param vCores CPU核心数
   */
  public void setvCores(int vCores) {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    this.vCores = vCores;
    resources.setVirtualCores(vCores);
  }

  /**
   * 获取原始Resource对象副本，延迟初始化内部Resource对象
   * @return 新建的Resource对象副本
   */
  public Resource getResource() {
    if (resources == null) {
      resources = Resource.newInstance(memory, vCores);
    }
    return Resource.newInstance(resources);
  }

  /**
   * 获取扩展资源信息集合
   * @return 扩展资源信息对象
   */
  public ResourceInformationsInfo getResourcesInformations() {
    return resourceInformations;
  }
}