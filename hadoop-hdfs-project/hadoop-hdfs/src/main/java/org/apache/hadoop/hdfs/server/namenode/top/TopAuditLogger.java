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
package org.apache.hadoop.hdfs.server.namenode.top;

import java.net.InetAddress;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;

import static org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics.TOPMETRICS_METRICS_SOURCE_NAME;

/**
 * 文件功能说明：
 * HDFS NameNode TOP服务审计日志实现类，将审计事件直接上报给TOP指标系统，
 * 用于统计并展示NameNode上TOP N最频繁的文件系统操作，帮助运维人员分析集群访问情况。
 * 
 * 实现了AuditLogger接口，作为NameNode审计日志的消费者，为TOP指标统计提供数据源。
 * 
 * 核心职责：接收NameNode产生的审计事件，转发给TOP指标系统进行统计分析。
 */
@InterfaceAudience.Private
public class TopAuditLogger implements AuditLogger {
  public static final Logger LOG = LoggerFactory.getLogger(TopAuditLogger.class);

  private final TopMetrics topMetrics;

  /**
   * 默认构造函数，自动初始化TOP指标系统并注册到Hadoop metrics系统
   */
  public TopAuditLogger() {
    Configuration conf = new HdfsConfiguration();
    TopConf topConf = new TopConf(conf);
    this.topMetrics = new TopMetrics(conf, topConf.nntopReportingPeriodsMs);
    // 检查指标源是否已注册，避免重复注册
    if (DefaultMetricsSystem.instance().getSource(
            TOPMETRICS_METRICS_SOURCE_NAME) == null) {
      DefaultMetricsSystem.instance().register(TOPMETRICS_METRICS_SOURCE_NAME,
              "Top N operations by user", topMetrics);
    }
  }

  /**
   * 带参数构造函数，使用外部注入的TopMetrics实例，主要用于测试
   * @param topMetrics 外部提供的TopMetrics指标实例
   */
  public TopAuditLogger(TopMetrics topMetrics) {
    Preconditions.checkNotNull(topMetrics, "Cannot init with a null " +
        "TopMetrics");
    this.topMetrics = topMetrics;
  }

  @Override
  /**
   * 初始化审计日志器，本实现不需要额外初始化逻辑
   * @param conf Hadoop配置对象
   */
  public void initialize(Configuration conf) {
  }

  @Override
  /**
   * 处理审计事件，将事件上报给TOP指标系统进行统计
   * @param succeeded 操作是否成功
   * @param userName 操作用户名
   * @param addr 客户端IP地址
   * @param cmd 操作命令类型
   * @param src 操作源路径
   * @param dst 操作目标路径
   * @param status 目标文件状态信息
   */
  public void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst, FileStatus status) {
    try {
      // 上报审计事件给TOP指标系统
      topMetrics.report(succeeded, userName, addr, cmd, src, dst, status);
    } catch (Throwable t) {
      // 捕获异常避免影响主审计流程，仅记录错误日志
      LOG.error("An error occurred while reflecting the event in top service, "
          + "event: (cmd={},userName={})", cmd, userName);
    }

    // debug级别日志输出完整事件信息
    if (LOG.isDebugEnabled()) {
      final StringBuilder sb = new StringBuilder();
      sb.append("allowed=").append(succeeded).append("\t");
      sb.append("ugi=").append(userName).append("\t");
      sb.append("ip=").append(addr).append("\t");
      sb.append("cmd=").append(cmd).append("\t");
      sb.append("src=").append(src).append("\t");
      sb.append("dst=").append(dst).append("\t");
      // 拼接权限信息
      if (null == status) {
        sb.append("perm=null");
      } else {
        sb.append("perm=");
        sb.append(status.getOwner()).append(":");
        sb.append(status.getGroup()).append(":");
        sb.append(status.getPermission());
      }
      LOG.debug("------------------- logged event for top service: " + sb);
    }
  }

}