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
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksum;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksumFactory;
import org.apache.hadoop.yarn.util.FSDownload;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 共享缓存上传任务可调用类，负责将本地化资源实际上传到YARN共享缓存
 * 当资源已存在或上传被拒绝时返回false，上传成功返回true
 */
class SharedCacheUploader implements Callable<Boolean> {
  // 共享缓存目录权限 rwxr-xr-x
  static final FsPermission DIRECTORY_PERMISSION =
      new FsPermission((short)00755);
  // 共享缓存文件权限 r-xr-xr-x
  static final FsPermission FILE_PERMISSION =
      new FsPermission((short)00555);

  private static final Logger LOG =
       LoggerFactory.getLogger(SharedCacheUploader.class);

  private final LocalResource resource;
  private final Path localPath;
  private final String user;
  private final Configuration conf;
  private final SCMUploaderProtocol scmClient;
  private final FileSystem fs;
  private final FileSystem localFs;
  private final String sharedCacheRootDir;
  private final int nestedLevel;
  private final SharedCacheChecksum checksum;
  private final RecordFactory recordFactory;

  public SharedCacheUploader(LocalResource resource, Path localPath,
      String user, Configuration conf, SCMUploaderProtocol scmClient)
          throws IOException {
    this(resource, localPath, user, conf, scmClient,
        FileSystem.get(conf), localPath.getFileSystem(conf));
  }

  /**
   * @param resource 包含原始远程路径的本地化资源
   * @param localPath 资源本地化后在本地文件系统的路径
   * @param user 提交应用的用户名
   * @param conf YARN配置对象
   * @param scmClient 共享缓存管理器客户端协议
   * @param fs 共享缓存所在的文件系统
   * @param localFs 节点本地文件系统
   */
  public SharedCacheUploader(LocalResource resource, Path localPath,
      String user, Configuration conf, SCMUploaderProtocol scmClient,
      FileSystem fs, FileSystem localFs) {
    this.resource = resource;
    this.localPath = localPath;
    this.user = user;
    this.conf = conf;
    this.scmClient = scmClient;
    this.fs = fs;
    this.sharedCacheRootDir =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    this.nestedLevel = SharedCacheUtil.getCacheDepth(conf);
    this.checksum = SharedCacheChecksumFactory.getChecksum(conf);
    this.localFs = localFs;
    this.recordFactory = RecordFactoryProvider.getRecordFactory(null);
  }

  /**
   * 执行共享缓存上传流程：权限验证、计算校验和、创建目录、上传临时文件、设置权限、重命名为最终文件、通知共享缓存管理器
   * 文件已存在或上传被拒绝则返回false，成功上传返回true
   */
  @Override
  public Boolean call() throws Exception {
    Path tempPath = null;
    try {
      // 验证用户是否有权限上传该资源
      if (!verifyAccess()) {
        LOG.warn("User " + user + " is not authorized to upload file " +
            localPath.getName());
        return false;
      }

      // 获取实际待上传的本地文件路径
      Path actualPath = getActualPath();
      // 计算文件校验和，作为共享缓存的资源键
      String checksumVal = computeChecksum(actualPath);
      // 根据嵌套层级和校验和计算共享缓存条目目录路径
      Path directoryPath =
          new Path(SharedCacheUtil.getCacheEntryPath(nestedLevel,
              sharedCacheRootDir, checksumVal));
      // 创建目录，已存在时也不会报错，直接复用
      fs.mkdirs(directoryPath, DIRECTORY_PERMISSION);
      // 生成临时文件路径
      tempPath = new Path(directoryPath, getTemporaryFileName(actualPath));
      // 执行文件上传到临时路径
      if (!uploadFile(actualPath, tempPath)) {
        LOG.warn("Could not copy the file to the shared cache at " + tempPath);
        return false;
      }

      // 设置文件权限为只读，符合共享缓存要求
      fs.setPermission(tempPath, FILE_PERMISSION);
      // 构造最终文件路径
      Path finalPath = new Path(directoryPath, actualPath.getName());
      // 将临时文件重命名为最终文件，原子操作，如果目标已存在则重命名失败
      if (!fs.rename(tempPath, finalPath)) {
        LOG.warn("The file already exists under " + finalPath +
            ". Ignoring this attempt.");
        deleteTempFile(tempPath);
        return false;
      }

      // 通知共享缓存管理器新资源上传完成
      if (!notifySharedCacheManager(checksumVal, actualPath.getName())) {
        // 共享缓存管理器拒绝本次上传，通常是因为已存在其他同名资源，需要清理已创建文件
        fs.delete(finalPath, false);
        return false;
      }

      // 根据配置设置共享缓存文件的副本数
      short replication =
          (short)conf.getInt(YarnConfiguration.SHARED_CACHE_NM_UPLOADER_REPLICATION_FACTOR,
              YarnConfiguration.DEFAULT_SHARED_CACHE_NM_UPLOADER_REPLICATION_FACTOR);
      fs.setReplication(finalPath, replication);
      LOG.info("File " + actualPath.getName() +
          " was uploaded to the shared cache at " + finalPath);
      return true;
    } catch (IOException e) {
      LOG.warn("Exception while uploading the file " + localPath.getName(), e);
      // 发生异常，清理临时文件
      deleteTempFile(tempPath);
      throw e;
    }
  }

  /**
   * 获取实际待上传的文件路径，处理解压后资源存放在同名子目录的情况
   */
  @VisibleForTesting
  Path getActualPath() throws IOException {
    Path path = localPath;
    FileStatus status = localFs.getFileStatus(path);
    if (status != null && status.isDirectory()) {
      // 对于解压后的资源，原始文件通常位于同名子目录下，参考FSDownload.unpack的逻辑
      path = new Path(path, path.getName());
    }
    return path;
  }

  private void deleteTempFile(Path tempPath) {
    try {
      if (tempPath != null) {
        fs.delete(tempPath, false);
      }
    } catch (IOException ioe) {
      LOG.debug("Exception received while deleting temp files", ioe);
    }
  }

  /**
   * 验证用户上传权限：公共资源直接允许；私有资源需要用户拥有或公开可读
   * 同时验证远程文件未被修改过，修改后的文件不允许上传
   */
  @VisibleForTesting
  boolean verifyAccess() throws IOException {
    // 公共资源直接允许上传
    if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
      return true;
    }

    final Path remotePath;
    try {
      // 从资源描述符获取原始远程路径
      remotePath = resource.getResource().toPath();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid resource", e);
    }

    // 获取原始远程文件的状态信息
    FileSystem remoteFs = remotePath.getFileSystem(conf);
    FileStatus status = remoteFs.getFileStatus(remotePath);
    // 检查本地化后原始文件是否被修改，修改过则拒绝上传
    if (status.getModificationTime() != resource.getTimestamp()) {
      LOG.warn("The remote file " + remotePath +
          " has changed since it's localized; will not consider it for upload");
      return false;
    }

    // 用户是文件所有者，允许上传
    if (status.getOwner().equals(user)) {
      return true;
    }
    // 否则检查文件是否公开可读，公开可读则允许上传
    return fileIsPublic(remotePath, remoteFs, status);
  }

  @VisibleForTesting
  boolean fileIsPublic(final Path remotePath, FileSystem remoteFs,
      FileStatus status) throws IOException {
    return FSDownload.isPublic(remoteFs, remotePath, status, null);
  }

  /**
   * 将本地源文件上传到共享缓存临时路径
   */
  @VisibleForTesting
  boolean uploadFile(Path sourcePath, Path tempPath) throws IOException {
    return FileUtil.copy(localFs, sourcePath, fs, tempPath, false, conf);
  }

  /**
   * 计算指定文件的校验和，作为共享缓存资源键
   */
  @VisibleForTesting
  String computeChecksum(Path path) throws IOException {
    InputStream is = localFs.open(path);
    try {
      return checksum.computeChecksum(is);
    } finally {
      try { is.close(); } catch (IOException ignore) {}
    }
  }

  // 生成带随机后缀的临时文件名，避免冲突
  private String getTemporaryFileName(Path path) {
    return path.getName() + "-" + ThreadLocalRandom.current().nextLong();
  }

  /**
   * 向共享缓存管理器发送上传完成通知，获取是否接受本次上传
   */
  @VisibleForTesting
  boolean notifySharedCacheManager(String checksumVal, String fileName)
      throws IOException {
    try {
      // 创建通知请求对象，设置校验和资源键与文件名
      SCMUploaderNotifyRequest request =
          recordFactory.newRecordInstance(SCMUploaderNotifyRequest.class);
      request.setResourceKey(checksumVal);
      request.setFilename(fileName);
      // 发送请求，返回是否接受
      return scmClient.notify(request).getAccepted();
    } catch (YarnException e) {
      throw new IOException(e);
    } catch (UndeclaredThrowableException e) {
      // 处理反射调用抛出的未声明异常，转为IOException抛出
      throw new IOException(e.getCause() == null ? e : e.getCause());
    }
  }
}