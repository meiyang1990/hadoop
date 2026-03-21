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
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN ResourceManager Web UI 应用统计信息数据访问对象
 * 封装按分组统计后的应用状态数量信息，用于REST接口返回统计结果
 */
@XmlRootElement(name = "appStatInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class ApplicationStatisticsInfo {

  // 存储分组后的统计项列表
  protected ArrayList<StatisticsItemInfo> statItem
      = new ArrayList<StatisticsItemInfo>();

  /**
   * 默认无参构造函数，供JAXB序列化/反序列化使用
   */
  public ApplicationStatisticsInfo() {
  } // JAXB needs this

  /**
   * 构造函数，从已有统计项集合初始化
   * @param items 统计项集合
   */
  public ApplicationStatisticsInfo(Collection<StatisticsItemInfo> items) {
    statItem.addAll(items);
  }

  /**
   * 添加单个统计项
   * @param statItem 待添加的统计项
   */
  public void add(StatisticsItemInfo statItem) {
    this.statItem.add(statItem);
  }

  /**
   * 获取所有统计项列表
   * @return 统计项列表
   */
  public ArrayList<StatisticsItemInfo> getStatItems() {
    return statItem;
  }

}