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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * 容量调度器自动创建叶子队列模板配置信息DAO，用于REST API返回模板配置数据
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class LeafQueueTemplateInfo {

  private ArrayList<ConfItem> property = new ArrayList<>();

  public LeafQueueTemplateInfo() {
  } // JAXB needs this

  /**
   * 从配置中解析提取指定父队列下的叶子队列模板配置
   * @param conf 资源管理器配置对象
   * @param queuePath 父队列路径
   */
  public LeafQueueTemplateInfo(Configuration conf, QueuePath queuePath) {
    // 拼接模板配置前缀路径
    String configPrefix = QueuePrefixes.
        getQueuePrefix(queuePath) + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
        + DOT;
    // 遍历所有配置项，过滤出匹配前缀的模板配置
    conf.forEach(entry -> {
      if (entry.getKey().startsWith(configPrefix)) {
        String name = entry.getKey();
        // 提取配置项短名称（去掉前缀部分）
        int start = name.lastIndexOf(AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
            + DOT);
        add(new ConfItem(name.substring(start), entry.getValue()));
      }
    });
  }

  public void add(ConfItem confItem) {
    property.add(confItem);
  }

  public ArrayList<ConfItem> getItems() {
    return property;
  }

  /**
   * 单个配置项存储类，保存配置键值对
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class ConfItem {

    private String name;
    private String value;

    public ConfItem() {
      // JAXB needs this
    }

    public ConfItem(String name, String value){
      this.name = name;
      this.value = value;
    }

    public String getKey() {
      return name;
    }

    public String getValue() {
      return value;
    }
  }
}