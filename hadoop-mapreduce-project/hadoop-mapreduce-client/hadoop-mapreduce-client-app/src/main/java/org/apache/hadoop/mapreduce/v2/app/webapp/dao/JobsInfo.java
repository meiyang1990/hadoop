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
 * Unless required by joblicable law or agreed to in writing, software
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
 * MapReduce WebUI 作业列表信息数据访问对象，用于序列化多个作业信息为JSON/XML格式
 * 供Web接口返回批量作业的列表数据
 */
@XmlRootElement(name = "jobs")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobsInfo {

  protected ArrayList<JobInfo> job = new ArrayList<JobInfo>();

  /**
   * 无参构造函数，供JAXB序列化/反序列化使用
   */
  public JobsInfo() {
  } // JAXB needs this

  /**
   * 添加单个作业信息到作业列表
   * @param jobInfo 单个作业的详细信息对象
   */
  public void add(JobInfo jobInfo) {
    job.add(jobInfo);
  }

  /**
   * 获取所有作业信息列表
   * @return 包含所有作业信息的ArrayList
   */
  public ArrayList<JobInfo> getJobs() {
    return job;
  }

}