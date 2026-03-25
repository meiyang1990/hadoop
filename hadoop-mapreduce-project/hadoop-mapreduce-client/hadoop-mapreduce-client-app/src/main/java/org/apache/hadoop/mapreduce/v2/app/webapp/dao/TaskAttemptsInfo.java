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
 * Unless required by taskattemptlicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 任务尝试尝试信息列表封装类，用于MapReduce应用Web服务的XML/JSON序列化
 * 存储单个任务下所有任务尝试的信息集合，供前端页面展示
 */
@XmlRootElement(name = "taskAttempts")
public class TaskAttemptsInfo {

  protected List<TaskAttemptInfo> taskAttempts = new ArrayList<>();

  /**
   * JAXB要求的无参构造函数，用于序列化/反序列化
   */
  public TaskAttemptsInfo() {
  } // JAXB needs this

  /**
   * 添加单个任务尝试信息到列表中
   * @param taskattemptInfo 单个任务尝试信息对象
   */
  public void add(TaskAttemptInfo taskattemptInfo) {
    taskAttempts.add(taskattemptInfo);
  }

  // XmlElementRef annotation should be used to identify the exact type of a list element
  // otherwise metadata will be added to XML attributes,
  // it can lead to incorrect JSON marshaling
  /**
   * 获取所有任务尝试信息列表
   * @return 任务尝试信息列表
   */
  @XmlElementRef
  public List<TaskAttemptInfo> getTaskAttempts() {
    return taskAttempts;
  }
}