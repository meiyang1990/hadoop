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
 * Unless required by tasklicable law or agreed to in writing, software
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

/**
 * MapReduce应用Web服务任务列表信息数据传输对象
 * 用于封装多个任务信息，在Web API中以XML/JSON格式返回任务列表数据
 */
@XmlRootElement(name = "tasks")
@XmlAccessorType(XmlAccessType.FIELD)
public class TasksInfo {

  protected ArrayList<TaskInfo> task = new ArrayList<TaskInfo>();

  /**
   * JAXB要求的无参构造函数，用于序列化/反序列化
   */
  public TasksInfo() {
  } // JAXB needs this

  /**
   * 添加单个任务信息到任务列表
   * @param taskInfo 要添加的单个任务信息对象
   */
  public void add(TaskInfo taskInfo) {
    task.add(taskInfo);
  }

  /**
   * 获取所有任务信息列表
   * @return 包含所有任务信息的ArrayList
   */
  public ArrayList<TaskInfo> getTasks() {
    return task;
  }

}