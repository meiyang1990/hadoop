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

package org.apache.hadoop.yarn.server.resourcemanager;


import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.eclipse.jetty.util.ajax.JSON;

/**
 * 文件说明：ResourceManager节点管理器信息JMX MBean实现
 * 核心职责：通过JMX暴露所有NodeManager的运行状态信息，供监控系统获取
 */
/**
 * JMX bean listing statuses of all node managers.
 */
public class RMNMInfo implements RMNMInfoBeans {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMNMInfo.class);
  // ResourceManager全局上下文对象，提供集群信息访问入口
  private RMContext rmContext;
  // ResourceManager资源调度器，获取节点资源使用信息
  private ResourceScheduler scheduler;
  // JMX MBean注册对象名，用于后续反注册
  private ObjectName mbeanObjectName;

  /**
   * 构造RMNMInfo实例，并将自身注册到JMX中
   * 
   * @param rmc resource manager's context object
   * @param sched resource manager's scheduler object
   */
  public RMNMInfo(RMContext rmc, ResourceScheduler sched) {
    this.rmContext = rmc;
    this.scheduler = sched;

    StandardMBean bean;
    try {
      // 包装为标准JMX MBean
      bean = new StandardMBean(this, RMNMInfoBeans.class);
      // 注册到MBean服务器
      mbeanObjectName = MBeans.register("ResourceManager", "RMNMInfo", bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering RMNMInfo MBean", e);
    }
    LOG.info("Registered RMNMInfo MBean");
  }

  /**
   * 从JMX反注册当前MBean，销毁时调用
   */
  public void unregister() {
    MBeans.unregister(mbeanObjectName);
  }

  /**
   * 用于存储节点信息的扩展LinkedHashMap，保持属性插入顺序
   */
  static class InfoMap extends LinkedHashMap<String, Object> {
    private static final long serialVersionUID = 1L;
  }

  /**
   * 获取所有活跃NodeManager的状态信息，返回JSON格式字符串
   * 
   * @return JSON formatted string containing statuses of all node managers
   */
  @Override // RMNMInfoBeans
  public String getLiveNodeManagers() {
    // 获取所有已注册节点集合
    Collection<RMNode> nodes = this.rmContext.getRMNodes().values();
    // 存储所有节点信息列表
    List<InfoMap> nodesInfo = new ArrayList<InfoMap>();

    // 遍历每个节点收集信息
    for (final RMNode ni : nodes) {
        // 从调度器获取节点资源使用报告
        SchedulerNodeReport report = scheduler.getNodeReport(ni.getNodeID());
        // 创建节点信息存储对象
        InfoMap info = new InfoMap();
        // 放入节点主机名
        info.put("HostName", ni.getHostName());
        // 放入节点机架信息
        info.put("Rack", ni.getRackName());
        // 放入节点运行状态
        info.put("State", ni.getState().toString());
        // 放入节点ID
        info.put("NodeId", ni.getNodeID());
        // 放入节点HTTP服务地址
        info.put("NodeHTTPAddress", ni.getHttpAddress());
        // 放入最后一次健康检查时间戳
        info.put("LastHealthUpdate",
                        ni.getLastHealthReportTime());
        // 放入健康检查报告内容
        info.put("HealthReport",
                        ni.getHealthReport());
        // 放入NodeManager版本号
        info.put("NodeManagerVersion",
                ni.getNodeManagerVersion());
        // 如果调度器返回了有效报告，添加资源使用信息
        if(report != null) {
          // 放入当前运行容器数量
          info.put("NumContainers", report.getNumContainers());
          // 放入已使用内存大小(MB)
          info.put("UsedMemoryMB", report.getUsedResource().getMemorySize());
          // 放入可用内存大小(MB)
          info.put("AvailableMemoryMB",
              report.getAvailableResource().getMemorySize());
        }

        // 将当前节点信息添加到结果列表
        nodesInfo.add(info);
    }

    // 将节点信息列表序列化为JSON字符串返回
    return JSON.toString(nodesInfo);
  }
}