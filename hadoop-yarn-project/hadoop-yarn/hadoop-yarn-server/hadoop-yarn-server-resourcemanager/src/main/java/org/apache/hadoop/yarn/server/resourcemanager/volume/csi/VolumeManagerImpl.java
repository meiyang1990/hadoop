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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * YARN ResourceManager端的CSI卷管理服务实现，负责管理集群中所有容器使用的CSI存储卷生命周期
 * 维护CSI驱动适配器缓存、卷状态信息，并调度卷制备异步任务
 */
public class VolumeManagerImpl extends AbstractService
    implements VolumeManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeManagerImpl.class);

  // 存储所有已声明卷的状态管理对象
  private final VolumeStates volumeStates;
  // 卷制备任务调度线程池
  private ScheduledExecutorService provisioningExecutor;
  // CSI驱动名称到对应适配器客户端的缓存映射
  private Map<String, CsiAdaptorProtocol> csiAdaptorMap;

  private final static int PROVISIONING_TASK_THREAD_POOL_SIZE = 10;

  /**
   * 构造VolumeManagerImpl实例，初始化内部缓存和线程池
   */
  public VolumeManagerImpl() {
    super(VolumeManagerImpl.class.getName());
    this.volumeStates = new VolumeStates();
    this.csiAdaptorMap = new ConcurrentHashMap<>();
    this.provisioningExecutor = Executors
        .newScheduledThreadPool(PROVISIONING_TASK_THREAD_POOL_SIZE);
  }

  // Init the CSI adaptor cache according to the configuration.
  // user only needs to configure a list of adaptor addresses,
  // this method extracts each address and init an adaptor client,
  // then proceed with a hand-shake by calling adaptor's getPluginInfo
  // method to retrieve the driver info. If the driver can be resolved,
  // it is then added to the cache. Note, we don't allow two drivers
  // specified with same driver-name even version is different.
  /**
   * 根据配置初始化CSI驱动适配器客户端缓存，完成与适配器的握手验证
   * @param adaptorMap 存储适配器客户端的缓存映射
   * @param conf Yarn配置对象
   * @throws IOException 网络IO异常
   * @throws YarnException 重复驱动或握手失败异常
   */
  private void initCsiAdaptorCache(
      final Map<String, CsiAdaptorProtocol> adaptorMap, Configuration conf)
      throws IOException, YarnException {
    LOG.info("Initializing cache for csi-driver-adaptors");
    // 从配置中获取所有CSI适配器地址列表
    String[] addresses =
        conf.getStrings(YarnConfiguration.NM_CSI_ADAPTOR_ADDRESSES);
    if (addresses != null && addresses.length > 0) {
      for (String addr : addresses) {
        LOG.info("Found csi-driver-adaptor socket address: " + addr);
        // 解析套接字地址
        InetSocketAddress address = NetUtils.createSocketAddr(addr);
        // 创建YARN RPC客户端
        YarnRPC rpc = YarnRPC.create(conf);
        // 获取当前用户凭证
        UserGroupInformation currentUser =
            UserGroupInformation.getCurrentUser();
        // 创建NodeManager代理客户端连接适配器
        CsiAdaptorProtocol adaptorClient = NMProxy
            .createNMProxy(conf, CsiAdaptorProtocol.class, currentUser, rpc,
                address);
        // Attempt to resolve the driver by contacting to
        // the diver's identity service on the given address.
        // If the call failed, the initialization is also failed
        // in order running into inconsistent state.
        LOG.info("Retrieving info from csi-driver-adaptor on address " + addr);
        // 调用getPluginInfo完成握手，获取驱动信息
        GetPluginInfoResponse response =
            adaptorClient.getPluginInfo(GetPluginInfoRequest.newInstance());
        if (!Strings.isNullOrEmpty(response.getDriverName())) {
          String driverName = response.getDriverName();
          // 检查是否存在重复驱动名称
          if (adaptorMap.containsKey(driverName)) {
            throw new YarnException(
                "Duplicate driver adaptor found," + " driver name: "
                    + driverName);
          }
          // 验证通过，将适配器加入缓存
          adaptorMap.put(driverName, adaptorClient);
          LOG.info("CSI Adaptor added to the cache, adaptor name: " + driverName
              + ", driver version: " + response.getVersion());
        }
      }
    }
  }

  /**
   * 根据驱动名称获取对应的CSI适配器客户端
   * @param driverName CSI驱动名称
   * @return 对应适配器客户端，未找到则返回null
   */
  public CsiAdaptorProtocol getAdaptorByDriverName(String driverName) {
    return csiAdaptorMap.get(driverName);
  }

  @VisibleForTesting
  @Override
  public void registerCsiDriverAdaptor(String driverName,
      CsiAdaptorProtocol client) {
    this.csiAdaptorMap.put(driverName, client);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    initCsiAdaptorCache(csiAdaptorMap, conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 关闭卷制备调度线程池
    provisioningExecutor.shutdown();
    super.serviceStop();
  }

  @Override
  public VolumeStates getVolumeStates() {
    return this.volumeStates;
  }

  @Override
  public Volume addOrGetVolume(Volume volume) {
    if (volumeStates.getVolume(volume.getVolumeId()) != null) {
      // 卷已存在，直接返回已有实例
      return volumeStates.getVolume(volume.getVolumeId());
    } else {
      // 新增卷到状态管理
      this.volumeStates.addVolumeIfAbsent(volume);
      return volume;
    }
  }

  @Override
  public ScheduledFuture<VolumeProvisioningResults> schedule(
      VolumeProvisioningTask volumeProvisioningTask,
      int delaySecond) {
    LOG.info("Scheduling provision volume task (with delay "
        + delaySecond + "s)," + " handling "
        + volumeProvisioningTask.getVolumes().size()
        + " volume provisioning");
    // 提交延迟任务到调度线程池
    return provisioningExecutor.schedule(volumeProvisioningTask,
        delaySecond, TimeUnit.SECONDS);
  }
}