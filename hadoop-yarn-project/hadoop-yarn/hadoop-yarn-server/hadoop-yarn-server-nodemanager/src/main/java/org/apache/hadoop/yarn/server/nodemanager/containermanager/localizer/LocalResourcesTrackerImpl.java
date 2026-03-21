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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;

import org.apache.hadoop.classification.VisibleForTesting;


/**
 * 同一可见性级别的{@link LocalizedResource}资源跟踪容器，负责管理节点上本地化资源的生命周期。
 * 跟踪资源下载、引用计数、缓存清理和NM重启后的资源恢复。
 */
class LocalResourcesTrackerImpl implements LocalResourcesTracker {

  static final Logger LOG =
       LoggerFactory.getLogger(LocalResourcesTrackerImpl.class);
  private static final String RANDOM_DIR_REGEX = "-?\\d+";
  private static final Pattern RANDOM_DIR_PATTERN = Pattern
      .compile(RANDOM_DIR_REGEX);

  private final String user;
  private final ApplicationId appId;
  private final Dispatcher dispatcher;
  @VisibleForTesting
  final ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc;
  private Configuration conf;
  private LocalDirsHandlerService dirsHandler;
  /*
   * This flag controls whether this resource tracker uses hierarchical
   * directories or not. For PRIVATE and PUBLIC resource trackers it
   * will be set whereas for APPLICATION resource tracker it would
   * be false.
   */
  private final boolean useLocalCacheDirectoryManager;
  private ConcurrentHashMap<Path, LocalCacheDirectoryManager> directoryManagers;
  /*
   * It is used to keep track of resource into hierarchical directory
   * while it is getting downloaded. It is useful for reference counting
   * in case resource localization fails.
   */
  private ConcurrentHashMap<LocalResourceRequest, Path>
    inProgressLocalResourcesMap;
  /*
   * starting with 10 to accommodate 0-9 directories created as a part of
   * LocalCacheDirectoryManager. So there will be one unique number generator
   * per APPLICATION, USER and PUBLIC cache.
   */
  private AtomicLong uniqueNumberGenerator = new AtomicLong(9);
  private NMStateStoreService stateStore;

  /**
   * 构造资源跟踪器，默认创建空的资源存储容器。
   * @param user 资源对应用户
   * @param appId 对应应用ID，应用级跟踪器为具体应用，公共/私有跟踪器为null
   * @param dispatcher 事件分发器
   * @param useLocalCacheDirectoryManager 是否启用分层缓存目录管理，PUBLIC/PRIVATE启用，APPLICATION不启用
   * @param conf 配置对象
   * @param stateStore NM状态存储服务，用于持久化资源状态
   * @param dirHandler 本地目录处理器，管理NM本地目录可用性
   */
  public LocalResourcesTrackerImpl(String user, ApplicationId appId,
      Dispatcher dispatcher, boolean useLocalCacheDirectoryManager,
      Configuration conf, NMStateStoreService stateStore,
      LocalDirsHandlerService dirHandler) {
    this(user, appId, dispatcher,
        new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>(),
        useLocalCacheDirectoryManager, conf, stateStore, dirHandler);
  }

  /**
   * 可指定资源存储容器的构造方法，用于测试和特殊场景。
   */
  LocalResourcesTrackerImpl(String user, ApplicationId appId,
      Dispatcher dispatcher,
      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc,
      boolean useLocalCacheDirectoryManager, Configuration conf,
      NMStateStoreService stateStore, LocalDirsHandlerService dirHandler) {
    this.appId = appId;
    this.user = user;
    this.dispatcher = dispatcher;
    this.localrsrc = localrsrc;
    this.useLocalCacheDirectoryManager = useLocalCacheDirectoryManager;
    if (this.useLocalCacheDirectoryManager) {
      directoryManagers =
          new ConcurrentHashMap<>();
      inProgressLocalResourcesMap =
          new ConcurrentHashMap<>();
    }
    this.conf = conf;
    this.stateStore = stateStore;
    this.dirsHandler = dirHandler;
  }

  /*
   * Synchronizing this method for avoiding races due to multiple ResourceEvent's
   * coming to LocalResourcesTracker from Public/Private localizer and
   * Resource Localization Service.
   */
  /**
   * 处理资源相关事件，同步避免多源事件并发冲突。
   */
  @Override
  public synchronized void handle(ResourceEvent event) {
    LocalResourceRequest req = event.getLocalResourceRequest();
    LocalizedResource rsrc = localrsrc.get(req);
    switch (event.getType()) {
    case LOCALIZED:
      // 本地化完成，从下载中地图移除
      if (useLocalCacheDirectoryManager) {
        inProgressLocalResourcesMap.remove(req);
      }
      break;
    case REQUEST:
      // 资源请求，如果资源不存在或者文件已丢失，重新创建资源
      if (rsrc != null && (!isResourcePresent(rsrc))) {
        LOG.info("Resource " + rsrc.getLocalPath()
            + " is missing, localizing it again");
        removeResource(req);
        rsrc = null;
      }
      if (null == rsrc) {
        rsrc = new LocalizedResource(req, dispatcher);
        localrsrc.put(req, rsrc);
      }
      break;
    case RELEASE:
      // 资源释放，资源不存在直接返回
      if (null == rsrc) {
        // The container sent a release event on a resource which 
        // 1) Failed
        // 2) Removed for some reason (ex. disk is no longer accessible)
        ResourceReleaseEvent relEvent = (ResourceReleaseEvent) event;
        LOG.info("Container " + relEvent.getContainer()
            + " sent RELEASE event on a resource request " + req
            + " not present in cache.");
        return;
      }
      break;
    case LOCALIZATION_FAILED:
      /*
       * If resource localization fails then Localized resource will be
       * removed from local cache.
       */
      // 本地化失败，移除缓存资源
      removeResource(req);
      break;
    case RECOVERED:
      // NM重启恢复资源，跳过已存在的资源
      if (rsrc != null) {
        LOG.warn("Ignoring attempt to recover existing resource " + rsrc);
        return;
      }
      rsrc = recoverResource(req, (ResourceRecoveredEvent) event);
      localrsrc.put(req, rsrc);
      break;
    }

    if (rsrc == null) {
      LOG.warn("Received " + event.getType() + " event for request " + req
          + " but localized resource is missing");
      return;
    }
    // 将事件转发给资源实例自身处理
    rsrc.handle(event);

    // Remove the resource if its downloading and its reference count has
    // become 0 after RELEASE. This maybe because a container was killed while
    // localizing and no other container is referring to the resource.
    // NOTE: This should NOT be done for public resources since the
    //       download is not associated with a container-specific localizer.
    // 释放后如果资源还在下载且引用计数归零，删除该资源（公共资源除外）
    if (event.getType() == ResourceEventType.RELEASE) {
      if (rsrc.getState() == ResourceState.DOWNLOADING &&
          rsrc.getRefCount() <= 0 &&
          rsrc.getRequest().getVisibility() != LocalResourceVisibility.PUBLIC) {
        removeResource(req);
      }
    }

    // 本地化完成后，持久化资源状态到状态存储
    if (event.getType() == ResourceEventType.LOCALIZED) {
      if (rsrc.getLocalPath() != null) {
        try {
          stateStore.finishResourceLocalization(user, appId,
              buildLocalizedResourceProto(rsrc));
        } catch (IOException ioe) {
          LOG.error("Error storing resource state for " + rsrc, ioe);
        }
      } else {
        LOG.warn("Resource " + rsrc + " localized without a location");
      }
    }
  }

  /**
   * 从恢复事件中重建LocalizedResource，更新唯一ID生成器避免冲突。
   */
  private LocalizedResource recoverResource(LocalResourceRequest req,
      ResourceRecoveredEvent event) {
    // 资源目录名就是资源ID，更新全局生成器保证不重复
    Path localDir = event.getLocalPath().getParent();
    long rsrcId = Long.parseLong(localDir.getName());

    // update ID generator to avoid conflicts with existing resources
    // CAS更新唯一ID生成器到不小于已恢复的最大ID
    while (true) {
      long currentRsrcId = uniqueNumberGenerator.get();
      long nextRsrcId = Math.max(currentRsrcId, rsrcId);
      if (uniqueNumberGenerator.compareAndSet(currentRsrcId, nextRsrcId)) {
        break;
      }
    }

    // 增加对应缓存目录的文件计数
    incrementFileCountForLocalCacheDirectory(localDir.getParent());

    return new LocalizedResource(req, dispatcher);
  }

  /**
   * 构建本地化资源的proto对象，用于持久化存储。
   */
  private LocalizedResourceProto buildLocalizedResourceProto(
      LocalizedResource rsrc) {
    return LocalizedResourceProto.newBuilder()
        .setResource(buildLocalResourceProto(rsrc.getRequest()))
        .setLocalPath(rsrc.getLocalPath().toString())
        .setSize(rsrc.getSize())
        .build();
  }

  /**
   * 构建LocalResource的proto对象，用于持久化。
   */
  private LocalResourceProto buildLocalResourceProto(LocalResource lr) {
    LocalResourcePBImpl lrpb;
    if (!(lr instanceof LocalResourcePBImpl)) {
      lr = LocalResource.newInstance(lr.getResource(), lr.getType(),
          lr.getVisibility(), lr.getSize(), lr.getTimestamp(),
          lr.getPattern());
    }
    lrpb = (LocalResourcePBImpl) lr;
    return lrpb.getProto();
  }

  /**
   * 对指定缓存目录增加文件计数，用于分层缓存目录的空间管理。
   */
  public void incrementFileCountForLocalCacheDirectory(Path cacheDir) {
    if (useLocalCacheDirectoryManager) {
      Path cacheRoot = LocalCacheDirectoryManager.getCacheDirectoryRoot(
          cacheDir);
      if (cacheRoot != null) {
        LocalCacheDirectoryManager dir = directoryManagers.get(cacheRoot);
        if (dir == null) {
          dir = new LocalCacheDirectoryManager(conf);
          LocalCacheDirectoryManager otherDir =
              directoryManagers.putIfAbsent(cacheRoot, dir);
          if (otherDir != null) {
            dir = otherDir;
          }
        }
        if (cacheDir.equals(cacheRoot)) {
          dir.incrementFileCountForPath("");
        } else {
          String dirStr = cacheDir.toUri().getRawPath();
          String rootStr = cacheRoot.toUri().getRawPath();
          dir.incrementFileCountForPath(
              dirStr.substring(rootStr.length() + 1));
        }
      }
    }
  }

  /*
   * Update the file-count statistics for a local cache-directory.
   * This will retrieve the localized path for the resource from
   * 1) inProgressRsrcMap if the resource was under localization and it
   * failed.
   * 2) LocalizedResource if the resource is already localized.
   * From this path it will identify the local directory under which the
   * resource was localized. Then rest of the path will be used to decrement
   * file count for the HierarchicalSubDirectory pointing to this relative
   * path.
   */
  /**
   * 对指定缓存目录减少文件计数，资源删除时更新统计。
   */
  private void decrementFileCountForLocalCacheDirectory(LocalResourceRequest req,
      LocalizedResource rsrc) {
    if ( useLocalCacheDirectoryManager) {
      Path rsrcPath = null;
      // 下载失败的资源从进行中地图获取路径
      if (inProgressLocalResourcesMap.containsKey(req)) {
        // This happens when localization of a resource fails.
        rsrcPath = inProgressLocalResourcesMap.remove(req);
      } else if (rsrc != null && rsrc.getLocalPath() != null) {
        // 已完成资源从资源对象获取路径
        rsrcPath = rsrc.getLocalPath().getParent().getParent();
      }
      if (rsrcPath != null) {
        Path parentPath = new Path(rsrcPath.toUri().getRawPath());
        // 向上查找找到对应目录管理器的缓存根
        while (!directoryManagers.containsKey(parentPath)) {
          parentPath = parentPath.getParent();
          if ( parentPath == null) {
            return;
          }
        }
        if ( parentPath != null) {
          String parentDir = parentPath.toUri().getRawPath().toString();
          LocalCacheDirectoryManager dir = directoryManagers.get(parentPath);
          String rsrcDir = rsrcPath.toUri().getRawPath(); 
          if (rsrcDir.equals(parentDir)) {
            dir.decrementFileCountForPath("");
          } else {
            dir.decrementFileCountForPath(
              rsrcDir.substring(
              parentDir.length() + 1));
          }
        }
      }
    }
  }

/**
   * 检查已本地化的资源文件是否真实存在于磁盘。
   * @param rsrc 待检查的本地化资源
   * @return 存在返回true，不存在返回false
   */
  public boolean isResourcePresent(LocalizedResource rsrc) {
    boolean ret = true;
    if (rsrc.getState() == ResourceState.LOCALIZED) {
      File file = new File(rsrc.getLocalPath().toUri().getRawPath().
        toString());
      if (!file.exists()) {
        ret = false;
      } else if (dirsHandler != null) {
        // 文件存在的情况下，检查所在目录是否是可用目录
        ret = checkLocalResource(rsrc);
      }
    }
    return ret;
  }

  /**
   * 检查资源所在目录是否是NM当前可用的本地目录。
   * @param rsrc 待检查资源
   * @return 目录可用返回true，否则返回false
   */
  @VisibleForTesting
  boolean checkLocalResource(LocalizedResource rsrc) {
    List<String> localDirs = dirsHandler.getLocalDirsForRead();
    for (String dir : localDirs) {
      if (isParent(rsrc.getLocalPath().toUri().getPath(), dir)) {
        return true;
      } else {
        continue;
      }
    }
    return false;
  }

  /**
   * 检查parentdir是否是path的父目录。
   * @param path 待检查路径
   * @param parentdir 父路径
   * @return 是父目录返回true，否则返回false
   */
  private boolean isParent(String path, String parentdir) {
    // Add separator if not present.
    if (path.charAt(path.length() - 1) != File.separatorChar) {
      path += File.separator;
    }
    return path.startsWith(parentdir);
  }

  @Override
  public boolean remove(LocalizedResource rem,