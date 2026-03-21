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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.apache.hadoop.util.Shell.getAllShells;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.DiskValidator;
import org.apache.hadoop.util.DiskValidatorFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.util.FSDownload;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 容器资源本地化器，负责在NodeManager节点上下载、本地化容器运行所需资源。
 * 作为独立进程运行，从ResourceManager申请资源并完成本地化后通知NodeManager。
 */
public class ContainerLocalizer {

  static final Logger LOG =
       LoggerFactory.getLogger(ContainerLocalizer.class);

  public static final String FILECACHE = "filecache";
  public static final String APPCACHE = "appcache";
  public static final String USERCACHE = "usercache";
  private static final String APPCACHE_CTXT_FMT = "%s.app.cache.dirs";
  private static final String USERCACHE_CTXT_FMT = "%s.user.cache.dirs";
  private static final FsPermission FILECACHE_PERMS =
      new FsPermission((short)0710);
  private static final FsPermission USERCACHE_FOLDER_PERMS =
      new FsPermission((short) 0755);
  public static final String CSI_VOLIUME_MOUNTS_ROOT = "csivolumes";

  /*
   * Testing discovered that these Java options are needed for Spark service
   * running on JDK17 and Isilon clusters.
   */
  // JDK17+版本需要额外添加的模块导出参数，解决Spark运行兼容性问题
  private static final String ADDITIONAL_JDK17_PLUS_OPTIONS =
    "--add-exports=java.base/sun.net.dns=ALL-UNNAMED " +
    "--add-exports=java.base/sun.net.util=ALL-UNNAMED";

  private final String user;
  private final String appId;
  private final List<Path> localDirs;
  private final String localizerId;
  private final FileContext lfs;
  private final Configuration conf;
  private final RecordFactory recordFactory;
  private final Map<LocalResource,Future<Path>> pendingResources;
  private final String appCacheDirContextName;
  private final DiskValidator diskValidator;

  private Set<Thread> localizingThreads =
      Collections.synchronizedSet(new HashSet<>());
  private final String tokenFileName;

  /**
   * 构造容器资源本地化器实例
   * @param lfs 本地文件系统上下文
   * @param user 本地化对应用户
   * @param appId 应用ID
   * @param localizerId 本地化器ID
   * @param tokenFileName 令牌文件路径
   * @param localDirs 本地目录列表
   * @param recordFactory 记录工厂
   * @throws IOException 初始化失败时抛出IO异常
   */
  public ContainerLocalizer(FileContext lfs, String user, String appId,
      String localizerId, String tokenFileName,  List<Path> localDirs,
      RecordFactory recordFactory) throws IOException {
    if (null == user) {
      throw new IOException("Cannot initialize for null user");
    }
    if (null == localizerId) {
      throw new IOException("Cannot initialize for null containerId");
    }
    this.lfs = lfs;
    this.user = user;
    this.appId = appId;
    this.localDirs = localDirs;
    this.localizerId = localizerId;
    this.recordFactory = recordFactory;
    this.conf = initConfiguration();
    this.diskValidator = DiskValidatorFactory.getInstance(
        YarnConfiguration.DEFAULT_DISK_VALIDATOR);
    this.appCacheDirContextName = String.format(APPCACHE_CTXT_FMT, appId);
    this.pendingResources = new HashMap<LocalResource,Future<Path>>();
    this.tokenFileName = Preconditions.checkNotNull(tokenFileName,
        "token file name cannot be null");
  }

  @VisibleForTesting
  @Private
  Configuration initConfiguration() {
    return new YarnConfiguration();
  }

  @Private
  @VisibleForTesting
  /**
   * 获取NodeManager本地化协议代理
   * @param nmAddr NodeManager地址
   * @return 本地化协议代理对象
   */
  public LocalizationProtocol getProxy(final InetSocketAddress nmAddr) {
    YarnRPC rpc = YarnRPC.create(conf);
    return (LocalizationProtocol)
      rpc.getProxy(LocalizationProtocol.class, nmAddr, conf);
  }

  @SuppressWarnings("deprecation")
  /**
   * 启动资源本地化主流程
   * @param nmAddr NodeManager地址
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void runLocalization(final InetSocketAddress nmAddr)
      throws IOException, InterruptedException {
    // 初始化本地缓存目录结构
    initDirs(conf, user, appId, lfs, localDirs);
    final Credentials creds = new Credentials();
    DataInputStream credFile = null;
    try {
      // 读取认证令牌文件
      Path tokenPath = new Path(tokenFileName);
      credFile = lfs.open(tokenPath);
      creds.readTokenStorageStream(credFile);
      // 本地化完成后删除令牌文件
      lfs.delete(tokenPath, false);      
    } finally  {
      if (credFile != null) {
        credFile.close();
      }
    }
    // 创建远程用户UGI并添加本地化令牌
    UserGroupInformation remoteUser =
      UserGroupInformation.createRemoteUser(user);
    remoteUser.addToken(creds.getToken(LocalizerTokenIdentifier.KIND));
    final LocalizationProtocol nodeManager =
        remoteUser.doAs(new PrivilegedAction<LocalizationProtocol>() {
          @Override
          public LocalizationProtocol run() {
            return getProxy(nmAddr);
          }
        });

    // 创建用户上下文UGI，添加所有认证令牌
    UserGroupInformation ugi =
      UserGroupInformation.createRemoteUser(user);
    for (Token<? extends TokenIdentifier> token : creds.getAllTokens()) {
      ugi.addToken(token);
    }

    ExecutorService exec = null;
    try {
      // 创建下载线程池和完成服务
      exec = createDownloadThreadPool();
      CompletionService<Path> ecs = createCompletionService(exec);
      // 开始本地化循环，处理NodeManager下发的资源下载任务
      localizeFiles(nodeManager, ecs, ugi);
    } catch (Throwable e) {
      throw new IOException(e);
    } finally {
      try {
        if (exec != null) {
          // 关闭线程池，销毁下载过程中启动的shell进程
          exec.shutdown();
          destroyShellProcesses(getAllShells());
          exec.awaitTermination(10, TimeUnit.SECONDS);
        }
        // 清除应用缓存目录上下文
        LocalDirAllocator.removeContext(appCacheDirContextName);
      } finally {
        // 关闭所有打开的文件系统
        closeFileSystems(ugi);
      }
    }
  }

  /**
   * 创建下载线程池，使用单线程处理下载
   * @return 线程池实例
   */
  ExecutorService createDownloadThreadPool() {
    return HadoopExecutors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setNameFormat("ContainerLocalizer Downloader-" + localizerId).build());
  }

  /**
   * 创建完成服务，用于处理异步下载结果
   * @param exec 线程池
   * @return 完成服务实例
   */
  CompletionService<Path> createCompletionService(ExecutorService exec) {
    return new ExecutorCompletionService<Path>(exec);
  }

  /**
   * FSDownload包装类，跟踪当前正在下载的线程
   */
  class FSDownloadWrapper extends FSDownload {

    FSDownloadWrapper(FileContext files, UserGroupInformation ugi,
        Configuration conf, Path destDirPath, LocalResource resource) {
      super(files, ugi, conf, destDirPath, resource);
    }

    @Override
    public Path call() throws Exception {
      Thread currentThread = Thread.currentThread();
      // 将当前下载线程加入跟踪集合
      localizingThreads.add(currentThread);
      try {
        return doDownloadCall();
      } finally {
        // 下载完成后移除线程跟踪
        localizingThreads.remove(currentThread);
      }
    }

    Path doDownloadCall() throws Exception {
      return super.call();
    }

  }

  /**
   * 创建资源下载任务
   * @param destDirPath 目标目录路径
   * @param rsrc 待下载资源
   * @param ugi 用户UGI
   * @return 可调用下载任务
   * @throws IOException IO异常
   */
  Callable<Path> download(Path destDirPath, LocalResource rsrc,
      UserGroupInformation ugi) throws IOException {
    // 私有资源需要提前创建父目录
    if (rsrc.getVisibility() == LocalResourceVisibility.PRIVATE) {
      createParentDirs(destDirPath);
    }
    // 检查目标磁盘健康状态
    diskValidator
        .checkStatus(new File(destDirPath.getParent().toUri().getRawPath()));
    return new FSDownloadWrapper(lfs, ugi, conf, destDirPath, rsrc);
  }

  /**
   * 创建私有资源的父目录结构
   * @param destDirPath 目标目录路径
   * @throws IOException IO异常
   */
  private void createParentDirs(Path destDirPath) throws IOException {
    Path parent = destDirPath.getParent();
    Path cacheRoot = LocalCacheDirectoryManager.getCacheDirectoryRoot(parent);
    Stack<Path> dirs = new Stack<Path>();
    // 从下往上收集需要创建的目录
    while (!parent.equals(cacheRoot)) {
      dirs.push(parent);
      parent = parent.getParent();
    }
    // 从上往下创建目录，应用用户缓存权限
    while (!dirs.isEmpty()) {
      createDir(lfs, dirs.pop(), USERCACHE_FOLDER_PERMS);
    }
  }

  /**
   * 估算资源本地化后需要的磁盘空间
   * @param rsrc 资源描述
   * @return 估算大小，单位字节
   */
  static long getEstimatedSize(LocalResource rsrc) {
    if (rsrc.getSize() < 0) {
      return -1;
    }
    switch (rsrc.getType()) {
      case ARCHIVE:
      case PATTERN:
        // 压缩包解压后一般会变大，估算5倍空间
        return 5 * rsrc.getSize();
      case FILE:
      default:
        return rsrc.getSize();
    }
  }

  /**
   * 睡眠指定秒数
   * @param duration 睡眠时间，单位秒
   * @throws InterruptedException 中断异常
   */
  void sleep(int duration) throws InterruptedException {
    TimeUnit.SECONDS.sleep(duration);
  }

  /**
   * 关闭用户对应的所有打开文件系统
   * @param ugi 用户UGI
   */
  protected void closeFileSystems(UserGroupInformation ugi) {
    try {
      FileSystem.closeAllForUGI(ugi);
    } catch (IOException e) {
      LOG.warn("Failed to close filesystems: ", e);
    }
  }

  /**
   * 本地化主循环，定期和NodeManager心跳，处理资源下载任务
   * @param nodemanager NodeManager本地化协议代理
   * @param cs 下载完成服务
   * @param ugi 用户UGI
   * @throws IOException IO异常
   * @throws YarnException YARN异常
   */
  protected void localizeFiles(LocalizationProtocol nodemanager,
      CompletionService<Path> cs, UserGroupInformation ugi)
      throws IOException, YarnException {
    while (true) {
      try {
        // 构造心跳状态
        LocalizerStatus status = createStatus();
        // 发送心跳到NodeManager，获取响应
        LocalizerHeartbeatResponse response = nodemanager.heartbeat(status);
        // 根据响应动作处理
        switch (response.getLocalizerAction()) {
        case LIVE:
          // 获取新的待本地化资源
          List<ResourceLocalizationSpec> newRsrcs = response.getResourceSpecs();
          for (ResourceLocalizationSpec newRsrc : newRsrcs) {
            // 避免重复提交同一个资源
            if (!pendingResources.containsKey(newRsrc.getResource())) {
              pendingResources.put(newRsrc.getResource(), cs.submit(download(
                new Path(newRsrc.getDestinationDirectory().getFile()),
                newRsrc.getResource(), ugi)));
            }
          }
          break;
        case DIE:
          // 取消所有正在进行的下载任务
          for (Future<Path> pending : pendingResources.values()) {
            pending.cancel(true);
          }
          status = createStatus();
          // 发送最终心跳，忽略响应
          try {
            nodemanager.heartbeat(status);
          } catch (YarnException e) {
            e.printStackTrace(System.out);
            LOG.error("Heartbeat failed while dying: ", e);
          }
          return;
        }
        // 等待下载完成，轮询间隔1秒
        cs.poll(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        return;
      } catch (YarnException e) {
        throw e;
      }
    }
  }

  /**
   * 创建心跳上报状态，包含所有资源当前本地化状态
   * @return 本地化器状态
   * @throws InterruptedException 中断异常
   */
  private LocalizerStatus createStatus() throws InterruptedException {
    final List<LocalResourceStatus> currentResources =
      new ArrayList<LocalResourceStatus>();
    // 遍历所有待处理资源，收集状态
    for (Iterator<Entry<LocalResource, Future<