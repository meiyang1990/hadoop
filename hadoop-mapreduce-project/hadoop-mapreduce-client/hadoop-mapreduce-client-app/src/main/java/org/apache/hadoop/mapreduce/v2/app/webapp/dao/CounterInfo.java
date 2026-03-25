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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.Counter;

/**
 * MapReduce作业计数器信息数据传输对象，用于Web UI展示计数器统计数据
 * 封装了总计数值、Map阶段计数值和Reduce阶段计数值，支持XML/JSON序列化
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class CounterInfo {

  // 计数器名称
  protected String name;
  // 计数器总数值
  protected long totalCounterValue;
  // Map阶段计数器数值
  protected long mapCounterValue;
  // Reduce阶段计数器数值
  protected long reduceCounterValue;

  /**
   * 默认无参构造函数，供JAXB序列化使用
   */
  public CounterInfo() {
  }

  /**
   * 构造函数，根据总计数器、Map阶段计数器、Reduce阶段计数器组装统计信息
   * @param c 总计数器对象，包含完整计数值
   * @param mc Map阶段计数器对象，可为空
   * @param rc Reduce阶段计数器对象，可为空
   */
  public CounterInfo(Counter c, Counter mc, Counter rc) {
    this.name = c.getName();
    this.totalCounterValue = c.getValue();
    this.mapCounterValue = mc == null ? 0 : mc.getValue();
    this.reduceCounterValue = rc == null ? 0 : rc.getValue();
  }
}