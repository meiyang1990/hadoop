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
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * 文件概要：MapReduce作业队列模型实现类，用于存储作业队列的配置信息、访问控制和层次结构
 * 存储作业队列的完整属性，支持层次化队列结构，支撑YARN队列调度和管理功能
 */
/**
 * 作业队列信息模型类，存储单个作业队列的所有属性与层次结构，支持多级嵌套队列
 * 用于MapReduce框架中管理作业提交队列的权限、状态和调度信息，支撑队列层次化组织与管理
 */
class Queue implements Comparable<Queue>{

  private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

  //队列名称
  private String name = null;

  //访问控制列表，key为操作类型，value为对应的权限控制
  private Map<String, AccessControlList> acls;

  //队列运行状态
  private QueueState state = QueueState.RUNNING;

  // 调度器可自定义存储的调度信息对象，框架会调用其toString方法生成UI展示文本
  private Object schedulingInfo;

  //子队列集合
  private Set<Queue> children;

  //队列自定义扩展属性
  private Properties props;

  /**
   * 默认构造函数，用于构造队列层次结构，后续通过setter方法填充属性
   */
  Queue() {
    
  }

  /**
   * 构造指定基础属性的作业队列对象
   * @param name 队列名称
   * @param acls 队列访问控制列表
   * @param state 队列运行状态
   */
  Queue(String name, Map<String, AccessControlList> acls, QueueState state) {
	  this.name = name;
	  this.acls = acls;
	  this.state = state;
  }
  
  /**
   * 获取队列名称
   * @return 队列名称
   */
  String getName() {
    return name;
  }
  
  /**
   * 设置队列名称
   * @param name 队列名称
   */
  void setName(String name) {
    this.name = name;
  }

  /**
   * 获取队列的访问控制列表
   * Map的key表示可执行的操作，value表示允许执行该操作的用户/组列表
   * @return 包含操作与对应权限的ACL映射
   */
  Map<String, AccessControlList> getAcls() {
    return acls;
  }
  
  /**
   * 设置队列的访问控制列表
   * @param acls 包含操作与对应权限的ACL映射
   */
  void setAcls(Map<String, AccessControlList> acls) {
    this.acls = acls;
  }
  
  /**
   * 获取队列当前运行状态
   * @return 队列运行状态
   */
  QueueState getState() {
    return state;
  }
  
  /**
   * 设置队列运行状态
   * @param state 队列运行状态
   */
  void setState(QueueState state) {
    this.state = state;
  }
  
  /**
   * 获取队列关联的调度信息
   * @return 调度器自定义的调度信息对象
   */
  Object getSchedulingInfo() {
    return schedulingInfo;
  }
  
  /**
   * 设置队列关联的调度信息
   * @param schedulingInfo 调度器自定义的调度信息对象
   */
  void setSchedulingInfo(Object schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  /**
   * 从源队列递归复制调度信息到当前队列及其所有子队列
   * @param sourceQueue 源队列，提供要复制的调度信息
   */
  void copySchedulingInfo(Queue sourceQueue) {
    // 先递归更新所有子队列的调度信息
    Set<Queue> destChildren = getChildren();
    if (destChildren != null) {
      Iterator<Queue> itr1 = destChildren.iterator();
      Iterator<Queue> itr2 = sourceQueue.getChildren().iterator();
      while (itr1.hasNext()) {
        itr1.next().copySchedulingInfo(itr2.next());
      }
    }

    // 复制当前队列自身的调度信息
    setSchedulingInfo(sourceQueue.getSchedulingInfo());
  }

  /**
   * 向当前队列添加子队列，初始化子队列集合若不存在
   * @param child 要添加的子队列
   */
  void addChild(Queue child) {
    if(children == null) {
      children = new TreeSet<Queue>();
    }

    children.add(child);
  }

  /**
   * 获取当前队列的所有子队列
   * @return 子队列集合，若没有子队列返回null
   */
  Set<Queue> getChildren() {
    return children;
  }

  /**
   * 设置队列自定义扩展属性
   * @param props 自定义属性集合
   */
  void setProperties(Properties props) {
     this.props = props;
  }

  /**
   * 获取队列自定义扩展属性
   * @return 自定义属性集合
   */
  Properties getProperties() {
    return this.props;
  }

  /**
   * 递归获取当前队列下所有拥有子队列的内部队列（非叶子节点）
   * 返回结果包含所有层级的内部队列，空结果返回空Map
   * @return 按队列名称索引的所有内部队列映射
   */
  Map<String,Queue> getInnerQueues() {
    Map<String,Queue> l = new HashMap<String,Queue>();

    // 没有子队列直接返回空Map
    if(children == null) {
      return l;
    }

    // 遍历检查所有子队列，递归收集内部队列
    for(Queue child:children) {
      // 如果子队列自身还有子队列，则加入结果并递归收集其内部队列
      if(child.getChildren() != null && child.getChildren().size() > 0) {
        l.put(child.getName(),child);
        l.putAll(child.getInnerQueues());
      }
    }
    return l;
  }

  /**
   * 递归获取当前队列层次下所有叶子队列（没有子队列的队列）
   * 当前队列本身如果是叶子节点则将自身加入结果，不会返回null
   * @return 按队列名称索引的所有叶子队列映射
   */
  Map<String,Queue> getLeafQueues() {
    Map<String,Queue> l = new HashMap<String,Queue>();
    if(children == null) {
      l.put(name,this);
      return l;
    }

    // 递归收集所有子队列下的叶子队列
    for(Queue child:children) {
      l.putAll(child.getLeafQueues());
    }
    return l;
  }


  @Override
  public int compareTo(Queue queue) {
    // 按队列名称字典序排序
    return name.compareTo(queue.getName());
  }
  
  @Override
  public boolean equals(Object o) {
    if(o == this) {
      return true;
    }
    if(! (o instanceof Queue)) {
      return false;
    }
    
    return ((Queue)o).getName().equals(name);
  }

  @Override
  public String toString() {
    return this.getName();
  }

  @Override
  public int hashCode() {
    return this.getName().hashCode();
  }

  /**
   * 将当前队列层次结构转换为JobQueueInfo对象，用于对外暴露队列信息
   * 递归转换所有子队列，深度复制自定义属性保证信息安全
   * @return 转换完成的根队列JobQueueInfo对象，包含所有子队列层次
   */
  JobQueueInfo getJobQueueInfo() {
    JobQueueInfo queueInfo = new JobQueueInfo();
    queueInfo.setQueueName(name);
    LOG.debug("created jobQInfo " + queueInfo.getQueueName());
    queueInfo.setQueueState(state.getStateName());
    if (schedulingInfo != null) {
      queueInfo.setSchedulingInfo(schedulingInfo.toString());
    }

    if (props != null) {
      // 深度复制属性对象，避免外部修改内部属性
      Properties newProps = new Properties();
      for (Object key : props.keySet()) {
        newProps.setProperty(key.toString(), props.getProperty(key.toString()));
      }
      queueInfo.setProperties(newProps);
    }

    if (children != null && children.size() > 0) {
      List<JobQueueInfo> list = new ArrayList<JobQueueInfo>();
      for (Queue child : children) {
        list.add(child.getJobQueueInfo());
      }
      queueInfo.setChildren(list);
    }
    return queueInfo;
  }

  /**
   * 递归检查当前队列的层次结构是否与新队列结构一致，用于队列刷新时验证结构变化
   * 检查队列名称、子队列数量和所有子队列的层次结构一致性
   * @param newState 新的队列状态对象，用于对比层次结构
   * @return 结构一致返回true，否则返回false
   */
  boolean isHierarchySameAs(Queue newState) {
    if(newState == null) {
      return false;
    }
    // 首先检查队列名称是否一致
    if(!(name.equals(newState.getName())) ) {
      LOG.info(" current name " + name + " not equal to " + newState.getName());
      return false;
    }

    if (children == null || children.size() == 0) {
      // 当前队列没有子队列，新队列有子队列则结构变化
      if(newState.getChildren() != null && newState.getChildren().size() > 0) {
        LOG.info( newState + " has added children in refresh ");
        return false;
      }
    } else if(children.size() > 0) {
      // 当前队列有子队列，检查新队列是否有对应子队列
      if (newState.getChildren() == null) {
        LOG.error("In the current state, queue " + getName() + " has "
            + children.size() + " but the new state has none!");
        return false;
      }
      int childrenSize = children.size();
      int newChildrenSize = newState.getChildren().size();
      // 子队列数量不一致则结构变化
      if (childrenSize != newChildrenSize) {
        LOG.error("Number of children for queue " + newState.getName()
            + " in newState is " + newChildrenSize + " which is not equal to "
            + childrenSize + " in the current state.");
        return false;
      }
      // 子队列存储在TreeSet中已按名称排序，顺序一致可直接遍历对比
      Iterator<Queue> itr1 = children.iterator();
      Iterator<Queue> itr2 = newState.getChildren().iterator();

      while(itr1.hasNext()) {
        Queue q = itr1.next();
        Queue newq = itr2.next();
        // 递归检查每个子队列结构
        if(! (q.isHierarchySameAs(newq)) ) {
          LOG.info(" Queue " + q.getName() + " not equal to " + newq.getName());
          return false;
        }
      }
    }
    // 所有检查通过，结构一致
    return true;
  }
}