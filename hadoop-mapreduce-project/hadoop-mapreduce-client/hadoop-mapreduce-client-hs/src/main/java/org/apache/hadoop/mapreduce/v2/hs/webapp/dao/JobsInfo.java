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
 * Unless required by applicable joblicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 历史服务器Web API中作业列表信息的数据传输对象
 * 用于封装所有已完成作业的信息集合，支持XML/JSON序列化返回给前端
 */
@XmlRootElement(name = "jobs")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobsInfo {

  protected ArrayList<JobInfo> job = new ArrayList<JobInfo>();

  /**
   * 默认无参构造方法，供JAXB序列化使用
   */
  public JobsInfo() {
  } // JAXB needs this

  /**
   * 添加单个作业信息到作业列表
   * @param jobInfo 单个作业的信息对象
   */
  public void add(JobInfo jobInfo) {
    this.job.add(jobInfo);
  }

  /**
   * 获取所有作业信息列表
   * @return 包含所有作业信息的ArrayList
   */
  public ArrayList<JobInfo> getJobs() {
    return this.job;
  }

}