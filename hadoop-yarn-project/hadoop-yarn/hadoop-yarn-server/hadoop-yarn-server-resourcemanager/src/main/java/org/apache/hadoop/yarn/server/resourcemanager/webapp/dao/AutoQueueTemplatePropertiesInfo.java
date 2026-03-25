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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LeafQueueTemplateInfo.ConfItem;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * 自动创建队列模板配置参数信息DAO，用于YARN ResourceManager Web UI返回自动队列模板配置信息
 * 封装自动创建队列的模板配置属性列表，支持XML序列化输出
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AutoQueueTemplatePropertiesInfo {

  private ArrayList<ConfItem> property =
      new ArrayList<>();

  public AutoQueueTemplatePropertiesInfo() {
  }

  /**
   * 获取所有模板配置项列表
   * @return 配置项列表
   */
  public ArrayList<ConfItem> getProperty() {
    return property;
  }

  /**
   * 添加一个模板配置项
   * @param confItem 要添加的配置项
   */
  public void add(ConfItem confItem) {
    property.add(confItem);
  }
}