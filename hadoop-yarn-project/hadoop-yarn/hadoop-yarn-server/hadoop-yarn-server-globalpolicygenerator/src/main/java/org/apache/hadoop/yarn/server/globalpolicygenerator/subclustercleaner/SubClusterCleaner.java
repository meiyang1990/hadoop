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

package org.apache.hadoop.yarn.server.globalpolicygenerator.subclustercleaner;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 子集群清理器是全局策略生成器(GPG)的服务之一，会定期检查联邦状态存储中的成员表，
 * 将超过指定时间未发送心跳的子集群标记为LOST状态。
 */
public class SubClusterCleaner implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(SubClusterCleaner.class);

  private GPGContext gpgContext;
  private long heartbeatExpirationMillis;

  /**
   * 构造子集群清理器，从配置读取心跳过期时间，初始化清理工具。
   * 该可执行任务会被子集群清理服务定时调用，检查并标记过期子集群。
   *
   * @param conf 配置对象
   * @param gpgContext 全局策略生成器上下文
   */
  public SubClusterCleaner(Configuration conf, GPGContext gpgContext) {
    this.heartbeatExpirationMillis = conf.getTimeDuration(
        YarnConfiguration.GPG_SUBCLUSTER_EXPIRATION_MS,
        YarnConfiguration.DEFAULT_GPG_SUBCLUSTER_EXPIRATION_MS, TimeUnit.MILLISECONDS);
    this.gpgContext = gpgContext;
    LOG.info("Initialized SubClusterCleaner with heartbeat expiration of {}",
        DurationFormatUtils.formatDurationISO(this.heartbeatExpirationMillis));
  }

  @Override
  public void run() {
    try {
      // 获取当前时间作为检查基准
      Date now = new Date();
      LOG.info("SubClusterCleaner at {}", now);

      // 从联邦状态存储获取所有子集群信息
      Map<SubClusterId, SubClusterInfo> infoMap =
          this.gpgContext.getStateStoreFacade().getSubClusters(false, true);

      // Iterate over each sub cluster and check last heartbeat
      for (Map.Entry<SubClusterId, SubClusterInfo> entry : infoMap.entrySet()) {
        SubClusterInfo subClusterInfo = entry.getValue();

        // 获取该子集群最后一次心跳时间
        Date lastHeartBeat = new Date(subClusterInfo.getLastHeartBeat());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking subcluster {} in state {}, last heartbeat at {}",
              subClusterInfo.getSubClusterId(), subClusterInfo.getState(),
              lastHeartBeat);
        }

        // 只检查当前可用状态的子集群
        if (subClusterInfo.getState().isUsable()) {
          // 计算距离过期还剩多少时间
          long timeUntilDeregister = this.heartbeatExpirationMillis
              - (now.getTime() - lastHeartBeat.getTime());
          // Deregister sub-cluster as SC_LOST if last heartbeat too old
          if (timeUntilDeregister < 0) {
            // 心跳已过期，将子集群标记为LOST状态并注销
            LOG.warn(
                "Deregistering subcluster {} in state {} last heartbeat at {}",
                subClusterInfo.getSubClusterId(), subClusterInfo.getState(),
                new Date(subClusterInfo.getLastHeartBeat()));
            try {
              this.gpgContext.getStateStoreFacade().deregisterSubCluster(
                  subClusterInfo.getSubClusterId(), SubClusterState.SC_LOST);
            } catch (Exception e) {
              LOG.error("deregisterSubCluster failed on subcluster "
                  + subClusterInfo.getSubClusterId(), e);
            }
          } else if (LOG.isDebugEnabled()) {
            // 未过期，debug日志输出剩余时间
            LOG.debug("Time until deregister for subcluster {}: {}",
                entry.getKey(),
                DurationFormatUtils.formatDurationISO(timeUntilDeregister));
          }
        }
      }
    } catch (Throwable e) {
      // 捕获所有异常，避免线程退出，保证定时任务持续运行
      LOG.error("Subcluster cleaner fails: ", e);
    }
  }

}