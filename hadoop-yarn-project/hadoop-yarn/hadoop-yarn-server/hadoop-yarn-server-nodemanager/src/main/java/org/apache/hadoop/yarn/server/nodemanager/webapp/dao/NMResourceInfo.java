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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * NodeManager节点资源信息DAO，用于NodeManager Web UI序列化输出节点资源数据
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class NMResourceInfo {
  private long resourceValue;

  public NMResourceInfo() {} // JAXB needs this

  /**
   * 获取资源数值
   * @return 资源数值
   */
  public long getResourceValue() {
    return resourceValue;
  }

  /**
   * 设置资源数值
   * @param resourceValue 资源数值
   */
  public void setResourceValue(long resourceValue) {
    this.resourceValue = resourceValue;
  }
}