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
package org.apache.hadoop.hdfs.protocol;

import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.IntrusiveCollection.Element;

import org.apache.hadoop.util.Preconditions;

/**
 * HDFS NameNode端缓存指令类，跟踪缓存路径相关的状态信息
 * 
 * 该类是HDFS缓存管理的核心实现类，用于维护需要缓存的路径、缓存副本数、过期时间
 * 以及缓存进度统计信息，属于NameNode内部实现类，不属于公共API
 */
@InterfaceAudience.Private
public final class CacheDirective implements IntrusiveCollection.Element {
  private final long id;
  private final String path;
  private final short replication;
  private CachePool pool;
  private final long expiryTime;

  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;

  private Element prev;
  private Element next;

  /**
   * 从CacheDirectiveInfo构造缓存指令对象
   * @param info 缓存指令信息对象
   */
  public CacheDirective(CacheDirectiveInfo info) {
    this(
        info.getId(),
        info.getPath().toUri().getPath(),
        info.getReplication(),
        info.getExpiration().getAbsoluteMillis());
  }

  /**
   * 构造缓存指令对象
   * @param id 缓存指令唯一ID
   * @param path 需要缓存的HDFS路径
   * @param replication 缓存副本数
   * @param expiryTime 过期时间（Unix毫秒时间戳）
   */
  public CacheDirective(long id, String path,
      short replication, long expiryTime) {
    Preconditions.checkArgument(id > 0);
    this.id = id;
    this.path = Preconditions.checkNotNull(path);
    Preconditions.checkArgument(replication > 0);
    this.replication = replication;
    this.expiryTime = expiryTime;
  }

  /**
   * 获取缓存指令唯一ID
   * @return 缓存指令ID
   */
  public long getId() {
    return id;
  }

  /**
   * 获取需要缓存的路径
   * @return HDFS路径字符串
   */
  public String getPath() {
    return path;
  }

  /**
   * 获取缓存副本数
   * @return 副本数
   */
  public short getReplication() {
    return replication;
  }

  /**
   * 获取该指令所属的缓存池
   * @return 缓存池对象
   */
  public CachePool getPool() {
    return pool;
  }

  /**
   * 获取缓存指令过期时间
   * @return 过期时间，毫秒数，Unix纪元
   */
  public long getExpiryTime() {
    return expiryTime;
  }

  /**
   * 获取格式化后的过期时间字符串
   * @return ISO-8601格式的过期时间字符串
   */
  public String getExpiryTimeString() {
    return DFSUtil.dateToIso8601String(new Date(expiryTime));
  }

  /**
   * 将当前CacheDirective转换为对外的CacheDirectiveInfo对象
   * 始终使用绝对过期时间，不保留相对TTL
   * @return 构造完成的CacheDirectiveInfo对象
   */
  public CacheDirectiveInfo toInfo() {
    return new CacheDirectiveInfo.Builder().
        setId(id).
        setPath(new Path(path)).
        setReplication(replication).
        setPool(pool.getPoolName()).
        setExpiration(CacheDirectiveInfo.Expiration.newAbsolute(expiryTime)).
        build();
  }

  /**
   * 生成当前缓存指令的统计信息
   * @return 缓存指令统计对象
   */
  public CacheDirectiveStats toStats() {
    return new CacheDirectiveStats.Builder().
        setBytesNeeded(bytesNeeded).
        setBytesCached(bytesCached).
        setFilesNeeded(filesNeeded).
        setFilesCached(filesCached).
        setHasExpired(new Date().getTime() > expiryTime).
        build();
  }

  /**
   * 生成包含指令信息和统计信息的条目对象
   * @return 缓存指令条目对象
   */
  public CacheDirectiveEntry toEntry() {
    return new CacheDirectiveEntry(toInfo(), toStats());
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ id:").append(id).
      append(", path:").append(path).
      append(", replication:").append(replication).
      append(", pool:").append(pool).
      append(", expiryTime: ").append(getExpiryTimeString()).
      append(", bytesNeeded:").append(bytesNeeded).
      append(", bytesCached:").append(bytesCached).
      append(", filesNeeded:").append(filesNeeded).
      append(", filesCached:").append(filesCached).
      append(" }");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CacheDirective other = (CacheDirective)o;
    return id == other.id;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(id);
  }

  //
  // 缓存统计相关的getter和setter
  //

  /**
   * 重置当前缓存指令的所有统计数据
   */
  public void resetStatistics() {
    bytesNeeded = 0;
    bytesCached = 0;
    filesNeeded = 0;
    filesCached = 0;
  }

  /**
   * 获取还需要缓存的字节数
   * @return 待缓存字节数
   */
  public long getBytesNeeded() {
    return bytesNeeded;
  }

  /**
   * 增加待缓存字节数，同时更新所属缓存池的统计
   * @param bytes 新增字节数
   */
  public void addBytesNeeded(long bytes) {
    this.bytesNeeded += bytes;
    pool.addBytesNeeded(bytes);
  }

  /**
   * 获取已经缓存的字节数
   * @return 已缓存字节数
   */
  public long getBytesCached() {
    return bytesCached;
  }

  /**
   * 增加已缓存字节数，同时更新所属缓存池的统计
   * @param bytes 新增字节数
   */
  public void addBytesCached(long bytes) {
    this.bytesCached += bytes;
    pool.addBytesCached(bytes);
  }

  /**
   * 获取还需要缓存的文件数
   * @return 待缓存文件数
   */
  public long getFilesNeeded() {
    return filesNeeded;
  }

  /**
   * 增加待缓存文件数，同时更新所属缓存池的统计
   * @param files 新增文件数
   */
  public void addFilesNeeded(long files) {
    this.filesNeeded += files;
    pool.addFilesNeeded(files);
  }

  /**
   * 获取已经缓存的文件数
   * @return 已缓存文件数
   */
  public long getFilesCached() {
    return filesCached;
  }

  /**
   * 增加已缓存文件数，同时更新所属缓存池的统计
   * @param files 新增文件数
   */
  public void addFilesCached(long files) {
    this.filesCached += files;
    pool.addFilesCached(files);
  }

  //
  // IntrusiveCollection.Element 接口实现
  //

  @SuppressWarnings("unchecked")
  @Override // IntrusiveCollection.Element
  /**
   * 内部插入方法，将当前指令插入到缓存池的指令链表中
   */
  public void insertInternal(IntrusiveCollection<? extends Element> list,
      Element prev, Element next) {
    assert this.pool == null;
    this.pool = ((CachePool.DirectiveList)list).getCachePool();
    this.prev = prev;
    this.next = next;
  }

  @Override // IntrusiveCollection.Element
  /**
   * 设置链表前驱节点
   */
  public void setPrev(IntrusiveCollection<? extends Element> list, Element prev) {
    assert list == pool.getDirectiveList();
    this.prev = prev;
  }

  @Override // IntrusiveCollection.Element
  /**
   * 设置链表后继节点
   */
  public void setNext(IntrusiveCollection<? extends Element> list, Element next) {
    assert list == pool.getDirectiveList();
    this.next = next;
  }

  @Override // IntrusiveCollection.Element
  /**
   * 内部移除方法，从缓存池指令链表中移除当前指令
   */
  public void removeInternal(IntrusiveCollection<? extends Element> list) {
    assert list == pool.getDirectiveList();
    this.pool = null;
    this.prev = null;
    this.next = null;
  }

  @Override // IntrusiveCollection.Element
  /**
   * 获取链表前驱节点
   * @return 前驱节点
   */
  public Element getPrev(IntrusiveCollection<? extends Element> list) {
    if (list != pool.getDirectiveList()) {
      return null;
    }
    return this.prev;
  }

  @Override // IntrusiveCollection.Element
  /**
   * 获取链表后继节点
   * @return 后继节点
   */
  public Element getNext(IntrusiveCollection<? extends Element> list) {
    if (list != pool.getDirectiveList()) {
      return null;
    }
    return this.next;
  }

  @Override // IntrusiveCollection.Element
  /**
   * 判断当前指令是否属于指定链表
   * @return 是否在链表中
   */
  public boolean isInList(IntrusiveCollection<? extends Element> list) {
    return pool == null ? false : list == pool.getDirectiveList();
  }
};