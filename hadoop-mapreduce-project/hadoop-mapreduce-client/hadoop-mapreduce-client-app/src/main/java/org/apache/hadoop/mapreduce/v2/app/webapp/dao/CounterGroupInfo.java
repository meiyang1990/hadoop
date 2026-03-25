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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;

/**
 * MapReduce WebUI中计数器分组信息的数据传输对象，用于封装一组计数器信息，支持XML/JSON序列化
 * 用于在Web界面展示MapReduce任务的统计指标分组数据
 */
@XmlRootElement(name = "counterGroup")
@XmlAccessorType(XmlAccessType.FIELD)
public class CounterGroupInfo {

  protected String counterGroupName;
  @XmlElement(name = "counter")
  protected ArrayList<CounterInfo> counter;

  /**
   * 无参构造函数，供JAXB序列化使用
   */
  public CounterGroupInfo() {
  }

  /**
   * 构造计数器分组信息对象，整合Map、Reduce和总计数器数据
   * @param name 计数器分组名称
   * @param group 当前计数器分组（总分组）
   * @param mg Map阶段计数器分组
   * @param rg Reduce阶段计数器分组
   */
  public CounterGroupInfo(String name, CounterGroup group, CounterGroup mg,
      CounterGroup rg) {
    this.counterGroupName = name;
    this.counter = new ArrayList<CounterInfo>();

    // 遍历当前分组中所有计数器，构造包含Map/Reduce阶段数据的计数器信息对象
    for (Counter c : group) {
      Counter mc = mg == null ? null : mg.findCounter(c.getName());
      Counter rc = rg == null ? null : rg.findCounter(c.getName());
      CounterInfo cinfo = new CounterInfo(c, mc, rc);
      this.counter.add(cinfo);
    }
  }

}