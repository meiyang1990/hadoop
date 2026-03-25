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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 历史服务器Web DAO类，封装应用Master（AM）所有尝试尝试信息的集合
 * 用于将历史作业的AM尝试信息序列化为XML/JSON格式返回给前端
 */
@XmlRootElement(name = "jobAttempts")
@XmlAccessorType(XmlAccessType.FIELD)
public class AMAttemptsInfo {

  @XmlElement(name = "jobAttempt")
  protected ArrayList<AMAttemptInfo> attempt = new ArrayList<AMAttemptInfo>();

  /**
   * 默认无参构造方法，供JAXB序列化框架使用
   */
  public AMAttemptsInfo() {
  } // JAXB needs this

  /**
   * 添加一个AM尝试信息到集合中
   * @param info 单个AM尝试信息对象
   */
  public void add(AMAttemptInfo info) {
    this.attempt.add(info);
  }

  /**
   * 获取所有AM尝试信息的集合
   * @return 存储所有AM尝试信息的ArrayList
   */
  public ArrayList<AMAttemptInfo> getAttempts() {
    return this.attempt;
  }

}