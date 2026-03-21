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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceFile;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.util.FSDownload;
import org.apache.hadoop.util.Preconditions;

/**
 * NodeManager辅助服务管理类，负责管理YARN节点上的自定义辅助服务，支持静态配置加载和动态清单加载、热重载。
 * 辅助服务允许NodeManager扩展额外功能，如shuffle服务、分布式缓存等。
 */
public class AuxServices extends AbstractService
    implements ServiceStateChangeListener, EventHandler<AuxServicesEvent> {

  /** NM本地辅助服务存储目录名称 */
  public static final String NM_AUX_SERVICE_DIR = "nmAuxService";
  /** NM辅助服务目录权限 */
  public static final FsPermission NM_AUX_SERVICE_DIR_PERM =
      new FsPermission((short) 0700);

  /** 配置中服务类名的key */
  public static final String CLASS_NAME = "class.name";
  /** 配置中系统类列表的key */
  public static final String SYSTEM_CLASSES = "system.classes";

  /** 恢复状态存储根目录名称 */
  static final String STATE_STORE_ROOT_NAME = "nm-aux-services";

  private static final Logger LOG =
       LoggerFactory.getLogger(AuxServices.class);
  /** 待删除旧版本目录后缀 */
  private static final String DEL_SUFFIX = "_DEL_";

  /** 已注册的辅助服务映射，key为服务名称 */
  private final Map<String, AuxiliaryService> serviceMap;
  /** 已注册的辅助服务信息记录映射，key为服务名称 */
  private final Map<String, AuxServiceRecord> serviceRecordMap;
  /** 已启动服务的元数据映射，key为服务名称 */
  private final Map<String, ByteBuffer> serviceMetaData;
  /** 本地路径处理器，用于处理辅助服务的本地路径 */
  private final AuxiliaryLocalPathHandler auxiliaryLocalPathHandler;
  /** NM本地目录处理器，用于获取本地可写路径 */
  private final LocalDirsHandlerService dirsHandler;
  /** 删除服务，用于异步清理旧文件 */
  private final DeletionService delService;
  /** 当前NodeManager运行用户信息，用于权限校验 */
  private final UserGroupInformation userUGI;

  /** 状态存储目录权限 */
  private final FsPermission storeDirPerms = new FsPermission((short)0700);
  /** 状态存储根路径，恢复功能启用时使用 */
  private Path stateStoreRoot = null;
  /** 状态存储文件系统，本地文件系统 */
  private FileSystem stateStoreFs = null;

  /** 是否启用动态清单加载功能 */
  private volatile boolean manifestEnabled = false;
  /** 动态清单文件路径 */
  private volatile Path manifest;
  /** 动态清单文件所在文件系统 */
  private volatile FileSystem manifestFS;
  /** 清单自动重载定时器 */
  private Timer manifestReloadTimer;
  /** 清单自动重载任务 */
  private TimerTask manifestReloadTask;
  /** 清单自动重载间隔（毫秒） */
  private long manifestReloadInterval;
  /** 上次读取清单时的修改时间戳 */
  private long manifestModifyTS = -1;

  /** JSON解析器，用于解析动态清单文件 */
  private final ObjectMapper mapper;

  /** 服务名称正则校验表达式，只允许字母开头，字母数字下划线 */
  private final Pattern p = Pattern.compile("^[A-Za-z_]+[A-Za-z0-9_]*$");

  /**
   * 构造辅助服务管理器实例
   * @param auxiliaryLocalPathHandler 本地路径处理器
   * @param nmContext NodeManager上下文
   * @param deletionService 删除服务
   */
  AuxServices(AuxiliaryLocalPathHandler auxiliaryLocalPathHandler,
      Context nmContext, DeletionService deletionService) {
    super(AuxServices.class.getName());
    serviceMap =
      Collections.synchronizedMap(new HashMap<String, AuxiliaryService>());
    serviceRecordMap =
        Collections.synchronizedMap(new HashMap<String, AuxServiceRecord>());
    serviceMetaData =
      Collections.synchronizedMap(new HashMap<String,ByteBuffer>());
    this.auxiliaryLocalPathHandler = auxiliaryLocalPathHandler;
    this.dirsHandler = nmContext.getLocalDirsHandler();
    this.delService = deletionService;
    this.userUGI = getRemoteUgi();
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // Obtain services from configuration in init()
  }

  /**
   * 返回是否启用辅助服务动态清单/动态加载功能
   */
  public boolean isManifestEnabled() {
    return manifestEnabled;
  }

  /**
   * 添加服务到服务映射表，同步方法保证线程安全
   *
   * @param name aux service name
   * @param service aux service
   * @param serviceRecord aux service record
   */
  protected final synchronized void addService(String name,
      AuxiliaryService service, AuxServiceRecord serviceRecord) {
    LOG.info("Adding auxiliary service " + serviceRecord.getName() +
        " version " + serviceRecord.getVersion());
    serviceMap.put(name, service);
    serviceRecordMap.put(name, serviceRecord);
  }

  /**
   * 获取所有已注册的辅助服务集合
   */
  Collection<AuxiliaryService> getServices() {
    return Collections.unmodifiableCollection(serviceMap.values());
  }

  /**
   * Gets current aux service records.
   *
   * @return a collection of service records
   */
  public Collection<AuxServiceRecord> getServiceRecords() {
    return Collections.unmodifiableCollection(serviceRecordMap.values());
  }

  /**
   * 获取所有已启动辅助服务的元数据，拷贝返回避免并发修改
   * @return 元数据映射，key为服务名称，value为服务元数据字节缓存
   */
  public Map<String, ByteBuffer> getMetaData() {
    Map<String, ByteBuffer> metaClone = new HashMap<>(serviceMetaData.size());
    synchronized (serviceMetaData) {
      for (Entry<String, ByteBuffer> entry : serviceMetaData.entrySet()) {
        metaClone.put(entry.getKey(), entry.getValue().duplicate());
      }
    }
    return metaClone;
  }

  /**
   * 使用配置类加载器创建辅助服务实例
   *
   * @param service aux service record
   * @return auxiliary service
   */
  private AuxiliaryService createAuxServiceFromConfiguration(AuxServiceRecord
      service) {
    Configuration c = new Configuration(false);
    c.set(CLASS_NAME, getClassName(service));
    Class<? extends AuxiliaryService> sClass = c.getClass(CLASS_NAME,
        null, AuxiliaryService.class);

    if (sClass == null) {
      throw new YarnRuntimeException("No class defined for auxiliary " +
          "service" + service.getName());
    }
    return ReflectionUtils.newInstance(sClass, null);
  }

  /**
   * 使用自定义本地类路径创建辅助服务实例
   *
   * @param service aux service record
   * @param appLocalClassPath local class path
   * @param conf configuration
   * @return auxiliary service
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private AuxiliaryService createAuxServiceFromLocalClasspath(
      AuxServiceRecord service, String appLocalClassPath, Configuration conf)
      throws IOException, ClassNotFoundException {
    Preconditions.checkArgument(appLocalClassPath != null &&
        !appLocalClassPath.isEmpty(),
        "local classpath was null in createAuxServiceFromLocalClasspath");
    final String sName = service.getName();
    final String className = getClassName(service);

    if (service.getConfiguration() != null && service.getConfiguration()
        .getFiles().size() > 0) {
      throw new YarnRuntimeException("The aux service:" + sName
          + " has configured local classpath:" + appLocalClassPath
          + " and config files:" + service.getConfiguration().getFiles()
          + ". Only one of them should be configured.");
    }

    return AuxiliaryServiceWithCustomClassLoader.getInstance(conf, className,
        appLocalClassPath, getSystemClasses(service));
  }

  /**
   * 根据服务规范创建辅助服务实例，根据是否配置远程资源选择加载方式
   *
   * @param service aux service record
   * @param conf configuration
   * @param fromConfiguration true if from configuration, false if from manifest
   * @return auxiliary service
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private AuxiliaryService createAuxService(AuxServiceRecord service,
      Configuration conf, boolean fromConfiguration) throws IOException,
      ClassNotFoundException {
    final String sName = service.getName();
    final String className = getClassName(service);
    if (className == null || className.isEmpty()) {
      throw new YarnRuntimeException("Class name not provided for auxiliary " +
          "service " + sName);
    }
    if (fromConfiguration) {
      // aux services from the configuration have an additional configuration
      // option specifying a local classpath that will not be localized
      final String appLocalClassPath = conf.get(String.format(
          YarnConfiguration.NM_AUX_SERVICES_CLASSPATH, sName));
      if (appLocalClassPath != null && !appLocalClassPath.isEmpty()) {
        return createAuxServiceFromLocalClasspath(service, appLocalClassPath,
            conf);
      }
    }
    AuxServiceConfiguration serviceConf = service.getConfiguration();
    List<Path> destFiles = new ArrayList<>();
    if (serviceConf != null) {
      List<AuxServiceFile> files = serviceConf.getFiles();
      if (files != null) {
        for (AuxServiceFile file : files) {
          // localize file (if needed) and add it to the list of paths that
          // will become the classpath
          destFiles.add(maybeDownloadJars(sName, className, file.getSrcFile(),
              file.getType(), conf));
        }
      }
    }

    if (destFiles.size() > 0) {
      // create aux service using a custom localized classpath
      LOG.info("The aux service:" + sName
          + " is using the custom classloader with classpath " + destFiles);
      return AuxiliaryServiceWithCustomClassLoader.getInstance(conf,
          className, StringUtils.join(File.pathSeparatorChar, destFiles),
          getSystemClasses(service));
    } else {
      return createAuxServiceFromConfiguration(service);
    }
  }

  /**
   * 下载远程辅助服务依赖文件到NM本地，版本变化时自动更新并删除旧版本
   * 如果文件已存在且版本未变化，直接返回本地路径
   *
   * @param sName service name
   * @param className service class name
   * @param remoteFile location of the file to download
   * @param type type of file (STATIC for a jar or ARCHIVE for a tarball)
   * @param conf configuration
   * @return path of the downloaded file
   * @throws IOException
   */
  @VisibleForTesting
  protected Path maybeDownloadJars(String sName, String className, String
      remoteFile, AuxServiceFile.TypeEnum type, Configuration conf)
      throws IOException {
    // 获取本地文件上下文
    FileContext localLFS = getLocalFileContext(conf);
    // create NM aux-service dir in NM localdir if it does not exist.
    // 在NM本地目录创建辅助服务存储目录
    Path nmAuxDir = dirsHandler.getLocalPathForWrite("."
        + Path.SEPARATOR + NM_AUX_SERVICE_DIR);
    if (!localLFS.util().exists(nmAuxDir)) {
      try {
        localLFS.mkdir(nmAuxDir, NM_AUX_SERVICE_DIR_PERM, true);
      } catch (IOException ex) {
        throw new YarnRuntimeException("Fail to create dir:"
            + nmAuxDir.toString(), ex);
      }
    }
    // 获取远程文件路径和文件上下文
    Path src = new Path(remoteFile);
    FileContext remoteLFS = getRemoteFileContext(src.toUri(), conf);
    FileStatus scFileStatus = remoteLFS.getFileStatus(src);
    // 校验远程文件所有者必须是NM运行用户
    if (!scFileStatus.getOwner().equals(
        this.userUGI.getShortUserName())) {
      throw new YarnRuntimeException("The remote jarfile owner:"
          + scFileStatus.getOwner() + " is not the same as the NM user:"
          + this.userUGI.getShortUserName() + ".");
    }
    // 校验远程文件不能允许组或其他用户写入，防止篡改
    if ((scFileStatus.getPermission().toShort() & 0022) != 0) {
      throw new YarnRuntimeException("The remote jarfile should not "
          + "be writable by group or others. "
          + "The current Permission is "
          + scFileStatus.getPermission().toShort());
    }
    // 根据类名和修改时间生成下载目标路径，实现版本区分
    Path downloadDest = new Path(nmAuxDir,
        className + "_" + scFileStatus.getModificationTime());
    // check whether we need to re-download the jar
    // from remote directory
    Path targetDirPath = new Path(downloadDest,
        scFileStatus.getPath().getName());
    // 遍历现有目录，检查是否已下载，同时标记旧版本为删除
    FileStatus[] allSubDirs = localLFS.util().listStatus(nmAuxDir);
    for (FileStatus sub : allSubDirs) {
      if (sub.getPath().getName().equals(downloadDest.getName())) {
        // 当前版本已存在，直接返回
        return targetDirPath;
      } else {
        // 标记同名旧版本为待删除，提交删除任务
        if (sub.getPath().getName().contains(className) &&
            !sub.getPath().getName().endsWith(DEL_SUFFIX)) {
          Path delPath = new Path(sub.getPath().getParent(),
              sub.getPath().getName() + DEL_SUFFIX);
          localLFS.rename(sub.getPath(), delPath);
          LOG.info("delete old aux service jar dir:"
              +