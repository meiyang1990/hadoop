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

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 容量调度器队列信息列表数据访问对象，用于YARN ResourceManager Web UI序列化返回队列列表数据
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerQueueInfoList {
  // 存储队列信息的列表
  protected ArrayList<CapacitySchedulerQueueInfo> queue;
  
  /**
   * 默认构造函数，初始化空队列列表
   */
  public CapacitySchedulerQueueInfoList() {
    queue = new ArrayList<>();
  }

  /**
   * 获取所有队列信息列表
   * @return 队列信息列表
   */
  public ArrayList<CapacitySchedulerQueueInfo> getQueueInfoList() {
    return this.queue;
  }
  
  /**
   * 添加一个队列信息到列表
   * @param e 待添加的队列信息对象
   * @return 添加成功返回true
   */
  public boolean addToQueueInfoList(CapacitySchedulerQueueInfo e) {
    return this.queue.add(e);
  }
  
  /**
   * 获取指定索引位置的队列信息
   * @param i 索引位置
   * @return 对应索引的队列信息对象
   */
  public CapacitySchedulerQueueInfo getQueueInfo(int i) {
    return this.queue.get(i);
  }
}