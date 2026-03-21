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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * RM Web UI 应用队列信息数据访问对象，封装应用所属队列信息，用于REST接口序列化返回。
 */
@XmlRootElement(name = "appqueue")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppQueue {

  String queue;

  public AppQueue() {
  }

  /**
   * 构造应用队列信息对象。
   * @param queue 队列名称
   */
  public AppQueue(String queue) {
    this.queue = queue;
  }

  /**
   * 设置队列名称。
   * @param queue 队列名称
   */
  public void setQueue(String queue) {
    this.queue = queue;
  }

  /**
   * 获取队列名称。
   * @return 队列名称
   */
  public String getQueue() {
    return this.queue;
  }

}