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
package org.apache.hadoop.hdfs.server.common;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * HDFS指标日志转储工具任务，定期将JMX暴露的Hadoop指标指标转储到日志文件，用于离线指标分析和问题排查。
 * 作为Runnable任务可被定时调度执行。
 */
public class MetricsLoggerTask implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(MetricsLoggerTask.class);

  private static ObjectName objectName = null;

  static {
    try {
      // 匹配所有Hadoop域下的MBean
      objectName = new ObjectName("Hadoop:*");
    } catch (MalformedObjectNameException m) {
      // 该异常不会触发，因为传入的是合法匹配模式
    }
  }

  private Logger metricsLog;
  private String nodeName;
  private short maxLogLineLength;

  /**
   * 构造指标日志转储任务，初始化日志记录器和配置参数。
   * @param metricsLog 指标日志的logger名称，用于输出转储指标
   * @param nodeName 当前节点名称（如NameNode/DataNode），用于日志标识
   * @param maxLogLineLength 单条指标日志最大长度，超长会被截断
   */
  public MetricsLoggerTask(String metricsLog, String nodeName, short maxLogLineLength) {
    this.metricsLog = LoggerFactory.getLogger(metricsLog);
    this.nodeName = nodeName;
    this.maxLogLineLength = maxLogLineLength;
  }

  /**
   * 执行指标转储任务，将所有Hadoop MBean指标查询后转储到指定日志。
   */
  @Override
  public void run() {
    // 如果日志未开启info级别、没有配置对应的appender或者MBean匹配模式为空，跳过本次转储
    if (!metricsLog.isInfoEnabled() || !hasAppenders(metricsLog)
        || objectName == null) {
      return;
    }

    metricsLog.info(" >> Begin " + nodeName + " metrics dump");
    // 获取平台MBean服务器，用于查询所有注册的MBean
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    // 遍历所有匹配的Hadoop MBean
    for (final ObjectName mbeanName : server.queryNames(objectName, null)) {
      try {
        // 获取MBean元信息
        MBeanInfo mBeanInfo = server.getMBeanInfo(mbeanName);
        // 获取MBean的名称标识
        final String mBeanNameName = MBeans.getMbeanNameName(mbeanName);
        // 过滤掉复杂类型属性，只保留可打印简单类型
        final Set<String> attributeNames = getFilteredAttributes(mBeanInfo);

        // 批量获取所有筛选后的属性值
        final AttributeList attributes = server.getAttributes(mbeanName,
            attributeNames.toArray(new String[attributeNames.size()]));

        // 遍历每个属性输出到日志
        for (Object o : attributes) {
          final Attribute attribute = (Attribute) o;
          final Object value = attribute.getValue();
          final String valueStr = (value != null) ? value.toString() : "null";
          // 超长截断后输出指标到日志
          metricsLog.info(mBeanNameName + ":" + attribute.getName() + "="
              + trimLine(valueStr));
        }
      } catch (Exception e) {
        // 获取指标失败记录错误日志
        metricsLog.error("Failed to get " + nodeName + " metrics for mbean "
            + mbeanName.toString(), e);
      }
    }
    metricsLog.info(" << End " + nodeName + " metrics dump");
  }

  /**
   * 按配置的最大长度截断指标字符串，超长部分用省略号替代。
   * @param valueStr 原始指标字符串
   * @return 截断后的字符串，如果长度未超过限制直接返回原字符串
   */
  private String trimLine(String valueStr) {
    if (maxLogLineLength <= 0) {
      return valueStr;
    }

    return (valueStr.length() < maxLogLineLength ? valueStr : valueStr
        .substring(0, maxLogLineLength) + "...");
  }

  // TODO : hadoop-logging module to hide log4j implementation details, this method
  //  can directly call utility from hadoop-logging.
  /**
   * 检查指定logger是否配置了appender，只有配置了才会执行指标转储。
   * @param logger 待检查的logger
   * @return true如果存在至少一个appender，否则返回false
   */
  private static boolean hasAppenders(Logger logger) {
    return org.apache.log4j.Logger.getLogger(logger.getName()).getAllAppenders()
        .hasMoreElements();
  }

  /**
   * 筛选MBean属性，过滤掉无法简单打印的复杂开放数据类型（TabularData、CompositeData），只保留简单类型属性。
   * @param mBeanInfo MBean元信息
   * @return 筛选后可打印属性名称集合
   */
  private static Set<String> getFilteredAttributes(MBeanInfo mBeanInfo) {
    Set<String> attributeNames = new HashSet<>();
    for (MBeanAttributeInfo attributeInfo : mBeanInfo.getAttributes()) {
      // 过滤掉所有复杂开放类型，只保留基本类型和简单可打印类型
      if (!attributeInfo.getType().equals(
          "javax.management.openmbean.TabularData")
          && !attributeInfo.getType().equals(
              "javax.management.openmbean.CompositeData")
          && !attributeInfo.getType().equals(
              "[Ljavax.management.openmbean.CompositeData;")) {
        attributeNames.add(attributeInfo.getName());
      }
    }
    return attributeNames;
  }

}