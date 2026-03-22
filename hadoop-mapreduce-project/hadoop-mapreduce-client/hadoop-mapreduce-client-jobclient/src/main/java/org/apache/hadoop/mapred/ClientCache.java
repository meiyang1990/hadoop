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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端缓存管理器，缓存并复用已创建的作业客户端代理，维护与ResourceManager和HistoryServer的RPC连接
 * 用于减少重复创建客户端连接的开销，提升作业客户端访问效率
 */
public class ClientCache {

  private final Configuration conf;
  private final ResourceMgrDelegate rm;

  private static final Logger LOG = LoggerFactory.getLogger(ClientCache.class);

  // 按JobID缓存对应的作业客户端代理，复用客户端实例
  private Map<JobID, ClientServiceDelegate> cache = 
      new HashMap<JobID, ClientServiceDelegate>();

  // 历史服务器客户端代理
  private MRClientProtocol hsProxy;

  /**
   * 构造客户端缓存实例
   * @param conf Hadoop配置对象
   * @param rm ResourceManager代理委托对象
   */
  public ClientCache(Configuration conf, ResourceMgrDelegate rm) {
    this.conf = conf;
    this.rm = rm;
  }

  //TODO: evict from the cache on some threshold
  /**
   * 根据作业ID获取对应的客户端代理，缓存未命中时新建并缓存
   * 首次调用时会初始化历史服务器代理连接
   * @param jobId 作业ID
   * @return 作业客户端代理实例
   */
  public synchronized ClientServiceDelegate getClient(JobID jobId) {
    if (hsProxy == null) {
      try {
        // 初始化历史服务器代理连接
        hsProxy = instantiateHistoryProxy();
      } catch (IOException e) {
        LOG.warn("Could not connect to History server.", e);
        throw new YarnRuntimeException("Could not connect to History server.", e);
      }
    }
    // 从缓存获取已存在的客户端代理
    ClientServiceDelegate client = cache.get(jobId);
    if (client == null) {
      // 缓存未命中，新建客户端并放入缓存
      client = new ClientServiceDelegate(conf, rm, jobId, hsProxy);
      cache.put(jobId, client);
    }
    return client;
  }

  /**
   * 获取已初始化的历史服务器代理，未初始化时先完成初始化
   * @return 初始化完成的历史服务器代理
   * @throws IOException 连接历史服务器失败时抛出IO异常
   */
  protected synchronized MRClientProtocol getInitializedHSProxy()
      throws IOException {
    if (this.hsProxy == null) {
      hsProxy = instantiateHistoryProxy();
    }
    return this.hsProxy;
  }
  
  /**
   * 根据配置创建历史服务器RPC代理连接
   * @return 历史服务器协议代理，未配置地址时返回null
   * @throws IOException 创建RPC连接失败时抛出IO异常
   */
  protected MRClientProtocol instantiateHistoryProxy()
      throws IOException {
    // 从配置中获取历史服务器地址
    final String serviceAddr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS);
    if (StringUtils.isEmpty(serviceAddr)) {
      return null;
    }
    LOG.debug("Connecting to HistoryServer at: " + serviceAddr);
    // 创建YARN RPC实例
    final YarnRPC rpc = YarnRPC.create(conf);
    LOG.debug("Connected to HistoryServer at: " + serviceAddr);
    // 获取当前用户信息，以当前用户身份创建代理
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    return currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
      @Override
      public MRClientProtocol run() {
        // 创建HSClientProtocol代理连接
        return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
      }
    });
  }

  /**
   * 关闭缓存，释放所有RPC连接和资源
   * @throws IOException 关闭资源时可能抛出IO异常
   */
  public void close() throws IOException {
    // 关闭ResourceManager代理连接
    if (rm != null) {
      rm.close();
    }

    // 停止历史服务器RPC代理，释放资源
    if (hsProxy != null) {
      RPC.stopProxy(hsProxy);
      hsProxy = null;
    }

    // 关闭所有缓存中的客户端代理，清空缓存
    if (cache != null && !cache.isEmpty()) {
      for (ClientServiceDelegate delegate : cache.values()) {
        if (delegate != null) {
          delegate.close();
          delegate = null;
        }
      }
      cache.clear();
      cache = null;
    }
  }
}