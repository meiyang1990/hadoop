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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.IntrusiveCollection;

import org.apache.hadoop.util.Preconditions;

/**
 * HDFS缓存池，用于在NameNode端管理一组缓存资源，用户的缓存请求会被计费到请求指定的缓存池。
 * 该类是NameNode内部使用类，对外暴露缓存池信息使用CachePoolInfo。
 * 所有访问必须在FSNamesystem锁下进行。
 */
@InterfaceAudience.Private
public final class CachePool {
  @Nonnull
  private final String poolName;

  @Nonnull
  private String ownerName;

  @Nonnull
  private String groupName;
  
  /**
   * 缓存池权限：
   * READ权限：允许列出缓存池中的缓存指令
   * WRITE权限：允许添加、删除、修改缓存池中的缓存指令
   * EXECUTE权限：未使用
   */
  @Nonnull
  private FsPermission mode;

  /**
   * 缓存池最大可缓存字节数限制
   */
  private long limit;

  /**
   * 缓存池中缓存指令的默认副本数
   */
  private short defaultReplication;

  /**
   * 缓存池中缓存指令的最大有效时长，单位毫秒
   */
  private long maxRelativeExpiryMs;

  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;

  /**
   * 缓存指令集合，用于管理当前缓存池中的所有缓存指令
   */
  public final static class DirectiveList
      extends IntrusiveCollection<CacheDirective> {
    private final CachePool cachePool;

    private DirectiveList(CachePool cachePool) {
      this.cachePool = cachePool;
    }

    public CachePool getCachePool() {
      return cachePool;
    }
  }

  @Nonnull
  private final DirectiveList directiveList = new DirectiveList(this);

  /**
   * 根据CachePoolInfo和默认值创建新缓存池，未填写的字段自动填充默认值
   * @param info 缓存池信息
   * @return 创建好的缓存池实例
   * @throws IOException 获取当前用户信息失败时抛出
   */
  static CachePool createFromInfoAndDefaults(CachePoolInfo info)
      throws IOException {
    UserGroupInformation ugi = null;
    String ownerName = info.getOwnerName();
    if (ownerName == null) {
      ugi = NameNode.getRemoteUser();
      ownerName = ugi.getShortUserName();
    }
    String groupName = info.getGroupName();
    if (groupName == null) {
      if (ugi == null) {
        ugi = NameNode.getRemoteUser();
      }
      groupName = ugi.getPrimaryGroupName();
    }
    FsPermission mode = (info.getMode() == null) ? 
        FsPermission.getCachePoolDefault() : info.getMode();
    long limit = info.getLimit() == null ?
        CachePoolInfo.DEFAULT_LIMIT : info.getLimit();
    short defaultReplication = info.getDefaultReplication() == null ?
        CachePoolInfo.DEFAULT_REPLICATION_NUM :
        info.getDefaultReplication();
    long maxRelativeExpiry = info.getMaxRelativeExpiryMs() == null ?
        CachePoolInfo.DEFAULT_MAX_RELATIVE_EXPIRY :
        info.getMaxRelativeExpiryMs();
    return new CachePool(info.getPoolName(),
        ownerName, groupName, mode, limit,
        defaultReplication, maxRelativeExpiry);
  }

  /**
   * 根据完整的CachePoolInfo创建新缓存池，要求所有字段都已填写
   * @param info 完整的缓存池信息
   * @return 创建好的缓存池实例
   */
  static CachePool createFromInfo(CachePoolInfo info) {
    return new CachePool(info.getPoolName(),
        info.getOwnerName(), info.getGroupName(),
        info.getMode(), info.getLimit(),
        info.getDefaultReplication(), info.getMaxRelativeExpiryMs());
  }

  /**
   * 构造缓存池实例
   * @param poolName 缓存池名称
   * @param ownerName 所有者用户名
   * @param groupName 所有者用户组
   * @param mode 权限模式
   * @param limit 缓存大小限制
   * @param defaultReplication 默认缓存副本数
   * @param maxRelativeExpiry 最大缓存有效时长
   */
  CachePool(String poolName, String ownerName, String groupName,
      FsPermission mode, long limit,
      short defaultReplication, long maxRelativeExpiry) {
    Preconditions.checkNotNull(poolName);
    Preconditions.checkNotNull(ownerName);
    Preconditions.checkNotNull(groupName);
    Preconditions.checkNotNull(mode);
    this.poolName = poolName;
    this.ownerName = ownerName;
    this.groupName = groupName;
    this.mode = new FsPermission(mode);
    this.limit = limit;
    this.defaultReplication = defaultReplication;
    this.maxRelativeExpiryMs = maxRelativeExpiry;
  }

  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public CachePool setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public CachePool setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  public FsPermission getMode() {
    return mode;
  }

  public CachePool setMode(FsPermission mode) {
    this.mode = new FsPermission(mode);
    return this;
  }

  public long getLimit() {
    return limit;
  }

  public CachePool setLimit(long bytes) {
    this.limit = bytes;
    return this;
  }

  public short getDefaultReplication() {
    return defaultReplication;
  }

  public void setDefaultReplication(short replication) {
    this.defaultReplication = replication;
  }

  public long getMaxRelativeExpiryMs() {
    return maxRelativeExpiryMs;
  }

  public CachePool setMaxRelativeExpiryMs(long expiry) {
    this.maxRelativeExpiryMs = expiry;
    return this;
  }

  /**
   * 获取缓存池信息，可选择返回完整信息或仅名称
   * @param fullInfo 是否返回完整信息
   * @return 缓存池信息对象
   */
  CachePoolInfo getInfo(boolean fullInfo) {
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (!fullInfo) {
      return info;
    }
    return info.setOwnerName(ownerName).
        setGroupName(groupName).
        setMode(new FsPermission(mode)).
        setLimit(limit).
        setDefaultReplication(defaultReplication).
        setMaxRelativeExpiryMs(maxRelativeExpiryMs);
  }

  /**
   * 重置缓存池的统计信息
   */
  public void resetStatistics() {
    bytesNeeded = 0;
    bytesCached = 0;
    filesNeeded = 0;
    filesCached = 0;
  }

  public void addBytesNeeded(long bytes) {
    bytesNeeded += bytes;
  }

  public void addBytesCached(long bytes) {
    bytesCached += bytes;
  }

  public void addFilesNeeded(long files) {
    filesNeeded += files;
  }

  public void addFilesCached(long files) {
    filesCached += files;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public long getBytesOverlimit() {
    return Math.max(bytesNeeded-limit, 0);
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public long getFilesCached() {
    return filesCached;
  }

  /**
   * 获取缓存池的统计信息
   * @return 缓存池统计对象
   */
  private CachePoolStats getStats() {
    return new CachePoolStats.Builder().
        setBytesNeeded(bytesNeeded).
        setBytesCached(bytesCached).
        setBytesOverlimit(getBytesOverlimit()).
        setFilesNeeded(filesNeeded).
        setFilesCached(filesCached).
        build();
  }

  /**
   * 根据调用用户权限获取缓存池条目，无权限用户只能看到基础信息
   * @param pc 权限检查器，可为null
   * @return 缓存池条目，包含信息和统计
   */
  public CachePoolEntry getEntry(FSPermissionChecker pc) {
    boolean hasPermission = true;
    if (pc != null) {
      try {
        pc.checkPermission(this, FsAction.READ);
      } catch (AccessControlException e) {
        hasPermission = false;
      }
    }
    return new CachePoolEntry(getInfo(hasPermission), 
        hasPermission ? getStats() : new CachePoolStats.Builder().build());
  }

  public String toString() {
    return new StringBuilder().
        append("{ ").append("poolName:").append(poolName).
        append(", ownerName:").append(ownerName).
        append(", groupName:").append(groupName).
        append(", mode:").append(mode).
        append(", limit:").append(limit).
        append(", defaultReplication").append(defaultReplication).
        append(", maxRelativeExpiryMs:").append(maxRelativeExpiryMs).
        append(" }").toString();
  }

  public DirectiveList getDirectiveList() {
    return directiveList;
  }
}