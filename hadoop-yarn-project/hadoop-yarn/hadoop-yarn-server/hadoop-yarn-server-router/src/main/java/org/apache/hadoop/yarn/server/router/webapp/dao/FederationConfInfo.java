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
package org.apache.hadoop.yarn.server.router.webapp.dao;

import org.apache.hadoop.yarn.webapp.dao.ConfInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * YARN Router联邦配置信息DAO类，用于封装所有子集群配置信息供Web接口返回
 * 继承自ConfInfo，扩展存储多个子集群配置和错误信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationConfInfo extends ConfInfo {
  // 存储所有子集群的配置信息列表
  @XmlElement(name = "subCluster")
  private List<ConfInfo> list = new ArrayList<>();

  // 存储收集配置过程中产生的错误信息列表
  @XmlElement(name = "errorMsgs")
  private List<String> errorMsgs = new ArrayList<>();

  /**
   * 无参构造函数，JAXB序列化反序列化需要
   */
  public FederationConfInfo() {
  } // JAXB needs this

  /**
   * 获取所有子集群配置信息列表
   * @return 子集群配置信息列表
   */
  public List<ConfInfo> getList() {
    return list;
  }

  public void setList(List<ConfInfo> list) {
    this.list = list;
  }

  /**
   * 获取收集配置过程中的错误信息列表
   * @return 错误信息列表
   */
  public List<String> getErrorMsgs() {
    return errorMsgs;
  }

  public void setErrorMsgs(List<String> errorMsgs) {
    this.errorMsgs = errorMsgs;
  }
}