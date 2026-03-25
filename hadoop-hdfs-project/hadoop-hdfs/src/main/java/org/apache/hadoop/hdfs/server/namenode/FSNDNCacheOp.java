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

import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.EnumSet;

/**
 * NameNode数据节点缓存操作工具类
 * 封装了HDFS缓存指令和缓存池的增删改查操作，统一处理权限检查和编辑日志记录
 */
class FSNDNCacheOp {
  /**
   * 添加缓存指令
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param directive 缓存指令信息
   * @param flags 缓存标志集合
   * @param logRetryCache 是否记录到重试缓存
   * @return 生效后的缓存指令信息
   * @throws IOException 权限校验或操作失败时抛出异常
   */
  static CacheDirectiveInfo addCacheDirective(
      FSNamesystem fsn, CacheManager cacheManager,
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags,
      boolean logRetryCache)
      throws IOException {

    final FSPermissionChecker pc = getFsPermissionChecker(fsn);

    if (directive.getId() != null) {
      throw new IOException("addDirective: you cannot specify an ID " +
          "for this operation.");
    }
    CacheDirectiveInfo effectiveDirective =
        cacheManager.addDirective(directive, pc, flags);
    fsn.getEditLog().logAddCacheDirectiveInfo(effectiveDirective,
        logRetryCache);
    return effectiveDirective;
  }

  /**
   * 修改已存在的缓存指令
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param directive 修改后的缓存指令信息
   * @param flags 缓存标志集合
   * @param logRetryCache 是否记录到重试缓存
   * @throws IOException 权限校验或操作失败时抛出异常
   */
  static void modifyCacheDirective(
      FSNamesystem fsn, CacheManager cacheManager, CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags, boolean logRetryCache) throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);

    cacheManager.modifyDirective(directive, pc, flags);
    fsn.getEditLog().logModifyCacheDirectiveInfo(directive, logRetryCache);
  }

  /**
   * 删除指定ID的缓存指令
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param id 要删除的缓存指令ID
   * @param logRetryCache 是否记录到重试缓存
   * @throws IOException 权限校验或操作失败时抛出异常
   */
  static void removeCacheDirective(
      FSNamesystem fsn, CacheManager cacheManager, long id,
      boolean logRetryCache)
      throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);

    cacheManager.removeDirective(id, pc);
    fsn.getEditLog().logRemoveCacheDirectiveInfo(id, logRetryCache);
  }

  /**
   * 分页批量列出缓存指令
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param startId 起始ID，用于分页
   * @param filter 过滤条件
   * @return 批量缓存指令列表结果
   * @throws IOException 权限校验或操作失败时抛出异常
   */
  static BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(
      FSNamesystem fsn, CacheManager cacheManager,
      long startId, CacheDirectiveInfo filter) throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);
    return cacheManager.listCacheDirectives(startId, filter, pc);
  }

  /**
   * 添加缓存池
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param req 缓存池请求信息
   * @param logRetryCache 是否记录到重试缓存
   * @return 生效后的缓存池信息
   * @throws IOException 操作失败时抛出异常
   */
  static CachePoolInfo addCachePool(
      FSNamesystem fsn, CacheManager cacheManager, CachePoolInfo req,
      boolean logRetryCache)
      throws IOException {
    CachePoolInfo info = cacheManager.addCachePool(req);
    fsn.getEditLog().logAddCachePool(info, logRetryCache);
    return info;
  }

  /**
   * 修改已存在的缓存池
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param req 修改后的缓存池信息
   * @param logRetryCache 是否记录到重试缓存
   * @throws IOException 操作失败时抛出异常
   */
  static void modifyCachePool(
      FSNamesystem fsn, CacheManager cacheManager, CachePoolInfo req,
      boolean logRetryCache) throws IOException {
    cacheManager.modifyCachePool(req);
    fsn.getEditLog().logModifyCachePool(req, logRetryCache);
  }

  /**
   * 删除指定名称的缓存池
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param cachePoolName 要删除的缓存池名称
   * @param logRetryCache 是否记录到重试缓存
   * @throws IOException 操作失败时抛出异常
   */
  static void removeCachePool(
      FSNamesystem fsn, CacheManager cacheManager, String cachePoolName,
      boolean logRetryCache) throws IOException {
    cacheManager.removeCachePool(cachePoolName);
    fsn.getEditLog().logRemoveCachePool(cachePoolName, logRetryCache);
  }

  /**
   * 分页批量列出缓存池
   * @param fsn NameNode文件系统对象
   * @param cacheManager 缓存管理器
   * @param prevKey 上一页最后一个键，用于分页
   * @return 批量缓存池列表结果
   * @throws IOException 权限校验或操作失败时抛出异常
   */
  static BatchedListEntries<CachePoolEntry> listCachePools(
      FSNamesystem fsn, CacheManager cacheManager, String prevKey)
      throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);
    return cacheManager.listCachePools(pc, prevKey);
  }

  /**
   * 获取权限检查器，若权限未启用则返回null
   * @param fsn NameNode文件系统对象
   * @return 权限检查器实例，权限未启用时返回null
   * @throws AccessControlException 获取检查器失败时抛出异常
   */
  private static FSPermissionChecker getFsPermissionChecker(FSNamesystem fsn)
      throws AccessControlException {
    return fsn.isPermissionEnabled() ? fsn.getPermissionChecker() : null;
  }
}