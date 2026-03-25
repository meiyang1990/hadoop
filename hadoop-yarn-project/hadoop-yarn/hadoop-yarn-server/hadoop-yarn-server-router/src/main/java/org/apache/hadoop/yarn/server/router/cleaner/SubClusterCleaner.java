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
package org.apache.hadoop.yarn.server.router.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * YARN联邦Router子集群清理线程，用于定期检查子集群心跳超时情况，
 * 将超过过期时间未上报心跳的子集群标记为LOST状态，默认每分钟检查一次。
 */
public class SubClusterCleaner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SubClusterCleaner.class);
  private FederationStateStoreFacade federationFacade;
  private long heartbeatExpirationMillis;

  /**
   * 构造子集群清理器，从配置中读取心跳过期时间并初始化联邦状态存储门面。
   * @param conf YARN配置对象
   */
  public SubClusterCleaner(Configuration conf) {
    federationFacade = FederationStateStoreFacade.getInstance(conf);
    this.heartbeatExpirationMillis =
        conf.getTimeDuration(YarnConfiguration.ROUTER_SUBCLUSTER_EXPIRATION_TIME,
        YarnConfiguration.DEFAULT_ROUTER_SUBCLUSTER_EXPIRATION_TIME, TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    try {
      // 获取当前检查时间
      Date now = new Date();
      LOG.info("SubClusterCleaner at {}.", now);

      // 从联邦状态存储获取所有子集群信息
      Map<SubClusterId, SubClusterInfo> subClusters = federationFacade.getSubClusters(true);

      // 遍历所有子集群逐个检查心跳状态
      for (Map.Entry<SubClusterId, SubClusterInfo> subCluster : subClusters.entrySet()) {
        // 获取当前子集群的ID、信息、状态和最后心跳时间
        SubClusterId subClusterId = subCluster.getKey();
        SubClusterInfo subClusterInfo = subCluster.getValue();
        SubClusterState subClusterState = subClusterInfo.getState();
        long lastHeartBeatTime = subClusterInfo.getLastHeartBeat();

        // 只检查可用状态(NEW/RUNNING)的子集群，非可用状态跳过检查
        if (subClusterState.isUsable()) {
          // 计算距上次心跳的时间间隔
          long heartBeatInterval = now.getTime() - lastHeartBeatTime;
          try {
            // 如果心跳间隔超过配置的过期时间，注销该子集群并标记为LOST
            if (heartBeatInterval > heartbeatExpirationMillis) {
              LOG.info("Deregister SubCluster {} in state {} last heartbeat at {}.",
                  subClusterId, subClusterState, new Date(lastHeartBeatTime));
              federationFacade.deregisterSubCluster(subClusterId, SubClusterState.SC_LOST);
            }
          } catch (YarnException e) {
            LOG.error("deregisterSubCluster failed on SubCluster {}.", subClusterId, e);
          }
        } else {
          LOG.debug("SubCluster {} in state {} last heartbeat at {}, " +
              "heartbeat interval < 30mins, no need for Deregister.",
              subClusterId, subClusterState, new Date(lastHeartBeatTime));
        }
      }
    } catch (Throwable e) {
      LOG.error("SubClusterCleaner Fails.", e);
    }
  }
}