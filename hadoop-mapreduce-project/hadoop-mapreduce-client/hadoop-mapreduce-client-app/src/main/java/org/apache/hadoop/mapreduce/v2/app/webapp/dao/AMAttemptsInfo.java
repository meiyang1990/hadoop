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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * MR ApplicationMaster所有任务尝试信息的Web数据访问对象，用于打包输出所有任务尝试信息给Web UI，支持XML/JSON序列化
 */
@XmlRootElement(name = "jobAttempts")
@XmlAccessorType(XmlAccessType.FIELD)
public class AMAttemptsInfo {

  @XmlElement(name = "jobAttempt")
  protected ArrayList<AMAttemptInfo> attempt = new ArrayList<AMAttemptInfo>();

  /**
   * JAXB序列化要求的无参构造方法，用于反序列化创建对象
   */
  public AMAttemptsInfo() {
  } // JAXB needs this

  /**
   * 添加单个任务尝试信息到列表中
   * @param info 单个任务尝试信息对象
   */
  public void add(AMAttemptInfo info) {
    this.attempt.add(info);
  }

  /**
   * 获取所有任务尝试信息列表
   * @return 包含所有任务尝试信息的ArrayList
   */
  public ArrayList<AMAttemptInfo> getAttempts() {
    return this.attempt;
  }

}