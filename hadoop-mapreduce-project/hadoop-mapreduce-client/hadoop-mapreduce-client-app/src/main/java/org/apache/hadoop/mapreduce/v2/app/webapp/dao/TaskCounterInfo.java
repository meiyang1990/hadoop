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

/**
 * MapReduce任务计数器信息数据传输对象，用于在Web UI界面展示任务计数器数据
 */
@XmlRootElement(name = "counter")
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskCounterInfo {

  protected String name;
  protected long value;

  /**
   * 空构造函数，供JAXB序列化/反序列化使用
   */
  public TaskCounterInfo() {
  }

  /**
   * 构造任务计数器信息对象
   * @param name 计数器名称
   * @param value 计数器数值
   */
  public TaskCounterInfo(String name, long value) {
    this.name = name;
    this.value = value;
  }

  /**
   * 获取计数器名称
   * @return 计数器名称
   */
  public String getName() {
    return name;
  }

  /**
   * 获取计数器数值
   * @return 计数器数值
   */
  public long getValue() {
    return value;
  }
}