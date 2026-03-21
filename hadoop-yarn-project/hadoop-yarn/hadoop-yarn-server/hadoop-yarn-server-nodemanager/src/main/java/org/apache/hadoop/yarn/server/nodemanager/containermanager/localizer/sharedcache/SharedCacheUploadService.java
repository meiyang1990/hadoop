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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Private
@Unstable
/**
 * NodeManager端共享缓存上传服务，负责将本地化文件异步上传到共享缓存。
 * 上传为尽力而为机制，上传失败不影响任务正常运行，不视为致命错误。
 */
public class SharedCacheUploadService extends AbstractService implements
    EventHandler<SharedCacheUploadEvent> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheUploadService.class);

  // 服务是否启用标识
  private boolean enabled;
  // HDFS文件系统客户端
  private FileSystem fs;
  // 本地文件系统客户端
  private FileSystem localFs;
  // 上传任务线程池
  private ExecutorService uploaderPool;
  // 共享缓存管理器RPC客户端
  private SCMUploaderProtocol scmClient;

  /**
   * 构造函数，初始化服务。
   */
  public SharedCacheUploadService() {
    super(SharedCacheUploadService.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取共享缓存是否启用
    enabled = conf.getBoolean(YarnConfiguration.SHARED_CACHE_ENABLED,
        YarnConfiguration.DEFAULT_SHARED_CACHE_ENABLED);
    if (enabled) {
      // 获取配置的上传线程数
      int threadCount =
          conf.getInt(YarnConfiguration.SHARED_CACHE_NM_UPLOADER_THREAD_COUNT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_NM_UPLOADER_THREAD_COUNT);
      // 创建固定大小的上传线程池
      uploaderPool = HadoopExecutors.newFixedThreadPool(threadCount,
          new ThreadFactoryBuilder().
            setNameFormat("Shared cache uploader #%d").
            build());
      // 创建共享缓存管理器RPC客户端
      scmClient = createSCMClient(conf);
      try {
        // 获取默认文件系统实例
        fs = FileSystem.get(conf);
        // 获取本地文件系统实例
        localFs = FileSystem.getLocal(conf);
      } catch (IOException e) {
        LOG.error("Unexpected exception in getting the filesystem", e);
        throw new RuntimeException(e);
      }
    }
    super.serviceInit(conf);
  }

  /**
   * 创建共享缓存管理器RPC代理客户端。
   * @param conf 配置对象
   * @return SCM上传协议代理实例
   */
  private SCMUploaderProtocol createSCMClient(Configuration conf) {
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 从配置解析SCM上传服务地址
    InetSocketAddress scmAddress =
        conf.getSocketAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_PORT);
    // 获取RPC代理并返回
    return (SCMUploaderProtocol)rpc.getProxy(
        SCMUploaderProtocol.class, scmAddress, conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (enabled) {
      // 关闭线程池，停止接受新任务
      uploaderPool.shutdown();
      // 停止RPC代理，释放资源
      RPC.stopProxy(scmClient);
    }
    super.serviceStop();
  }

  @Override
  /**
   * 处理共享缓存上传事件，提交异步上传任务。
   * @param event 共享缓存上传事件，包含待上传资源列表
   */
  public void handle(SharedCacheUploadEvent event) {
    if (enabled) {
      // 获取事件中携带的待上传资源
      Map<LocalResourceRequest,Path> resources = event.getResources();
      // 遍历所有待上传资源，逐个提交上传任务
      for (Map.Entry<LocalResourceRequest,Path> e: resources.entrySet()) {
        // 创建单个资源上传任务实例
        SharedCacheUploader uploader =
            new SharedCacheUploader(e.getKey(), e.getValue(), event.getUser(),
                getConfig(), scmClient, fs, localFs);
        // 提交上传任务到线程池异步执行
        uploaderPool.submit(uploader);
      }
    }
  }

  /**
   * 获取服务是否启用标识。
   * @return 服务是否启用
   */
  public boolean isEnabled() {
    return enabled;
  }
}