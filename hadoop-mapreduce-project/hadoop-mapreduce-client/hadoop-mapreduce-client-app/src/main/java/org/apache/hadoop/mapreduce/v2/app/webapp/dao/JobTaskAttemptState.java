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
 * MapReduce应用Web服务DAO层，封装任务尝试状态信息
 * 用于为Web UI提供REST API响应数据，存储任务尝试的当前状态
 */
@XmlRootElement(name = "jobTaskAttemptState")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobTaskAttemptState {

  private String state;

  /**
   * 无参构造函数，供JAXB反序列化使用
   */
  public JobTaskAttemptState() {
  }

  /**
   * 带参数构造函数，创建任务尝试状态对象
   * @param state 任务尝试的状态字符串
   */
  public JobTaskAttemptState(String state) {
    this.state = state;
  }

  /**
   * 设置任务尝试状态
   * @param state 任务尝试的状态字符串
   */
  public void setState(String state) {
    this.state = state;
  }

  /**
   * 获取任务尝试状态
   * @return 任务尝试的状态字符串
   */
  public String getState() {
    return this.state;
  }
}