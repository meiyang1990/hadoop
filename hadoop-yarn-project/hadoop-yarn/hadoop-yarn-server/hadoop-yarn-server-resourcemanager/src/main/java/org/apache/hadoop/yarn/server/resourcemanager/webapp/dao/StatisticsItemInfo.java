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
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * YARN ResourceManager Web UI 应用统计数据项的数据传输对象
 * 按应用状态和应用类型分组统计应用数量，供REST API返回统计结果
 */
@XmlRootElement(name = "statItem")
@XmlAccessorType(XmlAccessType.FIELD)
public class StatisticsItemInfo {

  // 应用状态
  protected YarnApplicationState state;
  // 应用类型
  protected String type;
  // 该分组下的应用数量
  protected long count;

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public StatisticsItemInfo() {
  } // JAXB needs this

  /**
   * 构造统计数据项
   * @param state 应用状态
   * @param type 应用类型
   * @param count 该分组下应用数量
   */
  public StatisticsItemInfo(
      YarnApplicationState state, String type, long count) {
    this.state = state;
    this.type = type;
    this.count = count;
  }

  /**
   * 拷贝构造函数
   * @param info 待拷贝的统计数据项对象
   */
  public StatisticsItemInfo(StatisticsItemInfo info) {
    this.state = info.state;
    this.type = info.type;
    this.count = info.count;
  }

  /**
   * 获取应用状态
   * @return 应用状态
   */
  public YarnApplicationState getState() {
    return state;
  }

  /**
   * 获取应用类型
   * @return 应用类型
   */
  public String getType() {
    return type;
  }

  /**
   * 获取应用数量
   * @return 统计分组下的应用数量
   */
  public long getCount() {
    return count;
  }

  /**
   * 设置应用数量
   * @param count 统计分组下的应用数量
   */
  public void setCount(long count) {
    this.count = count;
  }
}