// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Map任务尝试的数据访问对象，用于Web界面展示Map任务尝试的信息
 * 继承自TaskAttemptInfo，固定类型为MAP类型任务
 */
@XmlRootElement(name = "taskAttempt")
@XmlType(name = "")
public class MapTaskAttemptInfo extends TaskAttemptInfo {

  /**
   * 无参构造函数，供JAXB序列化/反序列化使用
   */
  public MapTaskAttemptInfo() {
  }

  /**
   * 构造函数，根据任务尝试对象构建Map任务尝试信息
   * @param ta MapReduce任务尝试对象
   */
  public MapTaskAttemptInfo(TaskAttempt ta) {
    this(ta, false);
  }

  /**
   * 构造函数，根据任务尝试对象和运行状态构建Map任务尝试信息
   * @param ta MapReduce任务尝试对象
   * @param isRunning 任务尝试是否正在运行
   */
  public MapTaskAttemptInfo(TaskAttempt ta, Boolean isRunning) {
    super(ta, TaskType.MAP, isRunning);
  }
}