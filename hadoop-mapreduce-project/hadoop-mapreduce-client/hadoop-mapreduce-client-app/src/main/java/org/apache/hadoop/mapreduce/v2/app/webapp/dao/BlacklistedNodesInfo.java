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

import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.v2.app.AppContext;

/**
 * 被拉黑节点信息数据访问对象，用于MapReduce Application Web界面
 * 封装当前Application中被拉黑节点的信息，支持JSON/XML序列化
 */
@XmlRootElement(name = "blacklistednodesinfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class BlacklistedNodesInfo {
  // 存储被拉黑节点的地址集合
  private Set<String> blacklistedNodes;
  
  /**
   * 默认构造函数，供JAXB序列化使用
   */
  public BlacklistedNodesInfo() { }
  
  /**
   * 从应用上下文构造被拉黑节点信息对象
   * @param appContext MapReduce应用上下文，从中获取被拉黑节点列表
   */
  public BlacklistedNodesInfo(AppContext appContext) {
    blacklistedNodes = appContext.getBlacklistedNodes();
  }
  
  /**
   * 获取被拉黑节点集合
   * @return 被拉黑节点地址集合
   */
  public Set<String> getBlacklistedNodes() {
    return blacklistedNodes;
  }
}