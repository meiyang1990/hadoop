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
package org.apache.hadoop.mapreduce.filecache;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.security.Credentials;

/**
 * 文件级注释：客户端分布式缓存管理器，在作业提交阶段负责处理分布式缓存的配置，包括获取文件时间戳、检查缓存可见性、获取委派令牌等
 * 
 * Manages internal configuration of the cache by the client for job submission.
 */
@InterfaceAudience.Private
public class ClientDistributedCacheManager {

  /**
   * 获取待缓存文件的时间戳并写入配置，同时判断缓存文件/归档的可见性
   * 可见性判断规则：如果文件本身对其他用户有读权限，且所有上级目录对其他用户有执行权限，则该缓存为公开可见
   *
   * @param job 作业配置对象
   * @throws IOException 文件系统操作异常
   */
  public static void determineTimestampsAndCacheVisibilities(Configuration job)
  throws IOException {
    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    determineTimestampsAndCacheVisibilities(job, statCache);
  }

  /**
   * 获取待缓存文件的时间戳并写入配置，同时判断缓存可见性，复用已缓存的FileStatus
   * See ClientDistributedCacheManager#determineTimestampsAndCacheVisibilities(
   * Configuration).
   *
   * @param job 作业配置对象
   * @param statCache 已缓存的FileStatus对象映射
   * @throws IOException 文件系统操作异常
   */
  public static void determineTimestampsAndCacheVisibilities(Configuration job,
      Map<URI, FileStatus> statCache) throws IOException {
    determineTimestamps(job, statCache);
    determineCacheVisibilities(job, statCache);
  }

  /**
   * 获取所有待缓存文件的时间戳和文件大小，并写入作业配置供后续使用，供JobClient在添加完所有缓存文件后调用
   * 
   * This is an internal method!
   * 
   * @param job 作业配置对象
   * @param statCache 已缓存的FileStatus对象映射
   * @throws IOException 文件系统操作异常
   */
  public static void determineTimestamps(Configuration job,
      Map<URI, FileStatus> statCache) throws IOException {
    URI[] tarchives = JobContextImpl.getCacheArchives(job);
    if (tarchives != null) {
      FileStatus status = getFileStatus(job, tarchives[0], statCache);
      StringBuilder archiveFileSizes =
        new StringBuilder(String.valueOf(status.getLen()));
      StringBuilder archiveTimestamps =
        new StringBuilder(String.valueOf(status.getModificationTime()));
      for (int i = 1; i < tarchives.length; i++) {
        status = getFileStatus(job, tarchives[i], statCache);
        archiveFileSizes.append(",");
        archiveFileSizes.append(String.valueOf(status.getLen()));
        archiveTimestamps.append(",");
        archiveTimestamps.append(String.valueOf(status.getModificationTime()));
      }
      job.set(MRJobConfig.CACHE_ARCHIVES_SIZES, archiveFileSizes.toString());
      setArchiveTimestamps(job, archiveTimestamps.toString());
    }
  
    URI[] tfiles = JobContextImpl.getCacheFiles(job);
    if (tfiles != null) {
      FileStatus status = getFileStatus(job, tfiles[0], statCache);
      StringBuilder fileSizes =
        new StringBuilder(String.valueOf(status.getLen()));
      StringBuilder fileTimestamps = new StringBuilder(String.valueOf(
        status.getModificationTime()));
      for (int i = 1; i < tfiles.length; i++) {
        status = getFileStatus(job, tfiles[i], statCache);
        fileSizes.append(",");
        fileSizes.append(String.valueOf(status.getLen()));
        fileTimestamps.append(",");
        fileTimestamps.append(String.valueOf(status.getModificationTime()));
      }
      job.set(MRJobConfig.CACHE_FILES_SIZES, fileSizes.toString());
      setFileTimestamps(job, fileTimestamps.toString());
    }
  }
  
  /**
   * 为所有分布式缓存文件/归档获取对应的委派令牌，存入凭证，用于访问HDFS的权限认证
   * @param job 作业配置对象
   * @param credentials 凭证对象，用于存储获取到的委派令牌
   * @throws IOException 文件系统操作异常
   */
  public static void getDelegationTokens(Configuration job,
      Credentials credentials) throws IOException {
    URI[] tarchives = JobContextImpl.getCacheArchives(job);
    URI[] tfiles = JobContextImpl.getCacheFiles(job);
    
    int size = (tarchives!=null? tarchives.length : 0) + (tfiles!=null ? tfiles.length :0);
    Path[] ps = new Path[size];
    
    int i = 0;
    if (tarchives != null) {
      for (i=0; i < tarchives.length; i++) {
        ps[i] = new Path(tarchives[i].toString());
      }
    }
    
    if (tfiles != null) {
      for(int j=0; j< tfiles.length; j++) {
        ps[i+j] = new Path(tfiles[j].toString());
      }
    }
    
    TokenCache.obtainTokensForNamenodes(credentials, ps, job);
  }
  
  /**
   * 判断所有分布式缓存文件/归档的可见性（公开/私有），结果写入作业配置
   * 可见性判断规则：文件本身对其他用户有读权限，且所有上级目录对其他用户有执行权限则为公开
   * @param job 作业配置对象
   * @param statCache 已缓存的FileStatus对象映射
   * @throws IOException 文件系统操作异常
   */
  public static void determineCacheVisibilities(Configuration job,
      Map<URI, FileStatus> statCache) throws IOException {
    URI[] tarchives = JobContextImpl.getCacheArchives(job);
    if (tarchives != null) {
      StringBuilder archiveVisibilities =
        new StringBuilder(String.valueOf(isPublic(job, tarchives[0], statCache)));
      for (int i = 1; i < tarchives.length; i++) {
        archiveVisibilities.append(",");
        archiveVisibilities.append(String.valueOf(isPublic(job, tarchives[i], statCache)));
      }
      setArchiveVisibilities(job, archiveVisibilities.toString());
    }
    URI[] tfiles = JobContextImpl.getCacheFiles(job);
    if (tfiles != null) {
      StringBuilder fileVisibilities =
        new StringBuilder(String.valueOf(isPublic(job, tfiles[0], statCache)));
      for (int i = 1; i < tfiles.length; i++) {
        fileVisibilities.append(",");
        fileVisibilities.append(String.valueOf(isPublic(job, tfiles[i], statCache)));
      }
      setFileVisibilities(job, fileVisibilities.toString());
    }
  }
  
  /**
   * 将归档可见性结果写入配置，存储顺序和归档添加顺序一致
   * 
   * @param conf 作业配置对象
   * @param booleans 逗号分隔的可见性标识，true表示公开
   */
  static void setArchiveVisibilities(Configuration conf, String booleans) {
    conf.set(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES, booleans);
  }

  /**
   * 将文件可见性结果写入配置，存储顺序和文件添加顺序一致
   * 
   * @param conf 作业配置对象
   * @param booleans 逗号分隔的可见性标识，true表示公开
   */
  static void setFileVisibilities(Configuration conf, String booleans) {
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, booleans);
  }

  /**
   * 将归档时间戳写入配置，存储顺序和归档添加顺序一致
   * 
   * @param conf 作业配置对象
   * @param timestamps 逗号分隔的归档时间戳
   */
  static void setArchiveTimestamps(Configuration conf, String timestamps) {
    conf.set(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS, timestamps);
  }

  /**
   * 将文件时间戳写入配置，存储顺序和文件添加顺序一致
   * 
   * @param conf 作业配置对象
   * @param timestamps 逗号分隔的文件时间戳
   */
  static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, timestamps);
  }

  /**
   * 获取指定URI的FileStatus，优先从缓存获取，缓存未命中则从文件系统获取并缓存
   * @param job 作业配置对象
   * @param uri 目标文件URI
   * @param statCache FileStatus缓存
   * @return 目标文件的FileStatus对象
   * @throws IOException 文件系统操作异常
   */
  private static FileStatus getFileStatus(Configuration job, URI uri,
      Map<URI, FileStatus> statCache) throws IOException {
    FileSystem fileSystem = FileSystem.get(uri, job);
    return getFileStatus(fileSystem, uri, statCache);
  }

  /**
   * 判断指定URI的缓存文件是否为公开可见（对所有用户可见）
   * @param conf 配置对象
   * @param uri 待判断的文件URI
   * @param statCache FileStatus缓存
   * @return 公开可见返回true，否则返回false
   * @throws IOException 文件系统操作异常
   */
  static boolean isPublic(Configuration conf, URI uri,
      Map<URI, FileStatus> statCache) throws IOException {
    boolean isPublic = true;
    FileSystem fs = FileSystem.get(uri, conf);
    Path current = new Path(uri.getPath());
    current = fs.makeQualified(current);

    // 如果是通配符路径，只需要检查祖先目录的执行权限，否则除了祖先目录，还需要检查文件本身读权限
    if (!current.getName().equals(DistributedCache.WILDCARD)) {
      isPublic = checkPermissionOfOther(fs, current, FsAction.READ, statCache);
    }

    return isPublic &&
        ancestorsHaveExecutePermissions(fs, current.getParent(), statCache);
  }

  /**
   * 检查指定路径的所有祖先目录是否都对其他用户开放了执行权限，允许用户遍历目录树到达目标路径
   * @param fs 文件系统对象
   * @param path 目标路径
   * @param statCache FileStatus缓存
   * @return 所有祖先都有执行权限返回true，否则返回false
   * @throws IOException 文件系统操作异常
   */
  static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path,
      Map<URI, FileStatus> statCache) throws IOException {
    Path current = path;
    while (current != null) {
      // 路径中的所有子目录都需要对其他用户开放执行权限
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
        return false;
      }
      current = current.getParent();
    }
    return true;
  }

  /**
   * 检查指定路径对其他用户是否拥有给定的操作权限
   * @param fs 文件系统对象
   * @param path 目标路径
   * @param action 需要检查的操作权限
   * @param statCache FileStatus缓存
   * @return 其他用户拥有对应权限返回true，否则返回false
   * @throws IOException 文件系统操作异常
   */
  private static boolean checkPermissionOfOther(FileSystem fs, Path path,
      FsAction action, Map<URI, FileStatus> statCache) throws IOException {
    FileStatus status = getFileStatus(fs, path.toUri(), statCache);

    // 加密文件始终视为私有，原因：
    // 1. 加密文件需要以作业提交者身份下载，才能通过KMS权限检查
    // 2. 避免加密区内世界可读的文件被错误当作公开缓存，导致权限问题
    if (!status.isEncrypted()) {
      FsAction otherAction = status.getPermission().getOtherAction();
      if (otherAction.implies(action)) {
        return true;
      }
    }

    return false;
  }

  /**
   * 获取指定URI的FileStatus，处理通配符路径，复用缓存结果
   * @param fs 文件系统对象
   * @param uri 目标文件URI
   * @param statCache FileStatus缓存
   * @return 目标文件的FileStatus对象
   * @throws IOException 文件系统操作异常
   */
  private static FileStatus getFileStatus(FileSystem fs, URI uri,
      Map<URI, FileStatus> statCache) throws IOException {
    Path path = new Path(uri);

    if (path.getName().equals(DistributedCache.WILDCARD)) {
      path = path.getParent();
      uri = path.toUri();
    }

    FileStatus stat = statCache.get(uri);

    if (stat == null) {
      stat = fs.getFileStatus(path);
      statCache.put(uri, stat);
    }

    return stat;
  }
}