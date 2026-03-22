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

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;

/**
 * Task计数器分组信息数据访问对象，用于MapReduce应用WebUI展示任务计数器分组数据
 * 将原生CounterGroup转换为可序列化的JSON/XML格式，供前端接口返回
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskCounterGroupInfo {

  protected String counterGroupName;
  protected ArrayList<TaskCounterInfo> counter;

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public TaskCounterGroupInfo() {
  }

  /**
   * 从原生CounterGroup构造Task计数器分组信息，转换为WebUI可用的数据结构
   * @param name 计数器分组名称
   * @param group 原生MapReduce计数器分组对象
   */
  public TaskCounterGroupInfo(String name, CounterGroup group) {
    this.counterGroupName = name;
    this.counter = new ArrayList<TaskCounterInfo>();

    for (Counter c : group) {
      TaskCounterInfo cinfo = new TaskCounterInfo(c.getName(), c.getValue());
      this.counter.add(cinfo);
    }
  }
}