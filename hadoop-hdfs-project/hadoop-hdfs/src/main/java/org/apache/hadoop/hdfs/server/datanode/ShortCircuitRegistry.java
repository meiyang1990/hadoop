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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.nativeio.SharedFileDescriptorFactory;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.DomainSocketWatcher;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultimap;

/**
 * 管理DataNode上客户端短路读使用的共享内存段，协调客户端与DataNode之间的短路读状态信息
 * 
 * DFSClients向DataNode申请共享内存段，ShortCircuitRegistry负责生成和管理这些段。每个段有一个随机生成的128位全局唯一ID，
 * 每个共享内存段包含多个用于存储块状态信息的"槽位(Slot)"。
 *
 * 在执行短路读之前，DFS客户端需要通过REQUEST_SHORT_CIRCUIT_FDS操作向DataNode申请一对文件描述符。
 * 作为该操作的一部分，客户端会传递它打算用来存储副本状态信息的共享内存段ID，以及段内想要使用的槽位编号，槽位分配始终由客户端完成。
 *
 * 槽位用于在客户端和DataNode两端跟踪块的状态。当DataNode对块mlock后，对应副本的槽位会被标记为"可锚定"。
 * 可锚定块可以安全跳过校验和验证直接读取，因此使用这些副本的BlockReaderLocal可以跳过校验过程，
 * 同时也支持对这些副本执行零拷贝读取（零拷贝接口没有校验和验证能力）。
 * 
 * 当DataNode需要munlock块时，需要先等待块被正在执行无校验读或零拷贝读的客户端取消锚定。
 * DataNode还会将块对应槽位标记为"不可锚定"，防止后续客户端发起此类操作。
 * 
 * 该类在客户端侧的对应实现是 {@link DfsClientShmManager}。
 */
public class ShortCircuitRegistry {
  public static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitRegistry.class);

  private static final int SHM_LENGTH = 8192;

  /**
   * 已注册的共享内存段，负责处理域套接字关闭事件以清理资源
   */
  public static class RegisteredShm extends ShortCircuitShm
      implements DomainSocketWatcher.Handler {
    private final String clientName;
    private final ShortCircuitRegistry registry;

    RegisteredShm(String clientName, ShmId shmId, FileInputStream stream,
        ShortCircuitRegistry registry) throws IOException {
      super(shmId, stream);
      this.clientName = clientName;
      this.registry = registry;
    }

    @Override
    public boolean handle(DomainSocket sock) {
      synchronized (registry) {
        synchronized (this) {
          registry.removeShm(this);
        }
      }
      return true;
    }

    String getClientName() {
      return clientName;
    }
  }

  /**
   * 从注册表中移除指定共享内存段并清理所有关联资源
   * @param shm 要移除的共享内存段
   */
  public synchronized void removeShm(ShortCircuitShm shm) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("removing shm " + shm);
    }
    // 从段映射中删除该共享内存段
    RegisteredShm removedShm = segments.remove(shm.getShmId());
    Preconditions.checkState(removedShm == shm,
        "failed to remove " + shm.getShmId());
    // 清理该段上所有已分配槽位
    for (Iterator<Slot> iter = shm.slotIterator(); iter.hasNext(); ) {
      Slot slot = iter.next();
      boolean removed = slots.remove(slot.getBlockId(), slot);
      Preconditions.checkState(removed);
      slot.makeInvalid();
    }
    // 释放共享内存映射并关闭共享文件
    shm.free();
  }

  /**
   * 短路读注册表是否已启用
   */
  private boolean enabled;

  /**
   * 共享文件描述符工厂，用于创建新的共享内存段
   */
  private final SharedFileDescriptorFactory shmFactory;
  
  /**
   * 域套接字监视器，当关联共享内存段的UNIX域套接字关闭时触发回调清理资源
   */
  private final DomainSocketWatcher watcher;

  private final HashMap<ShmId, RegisteredShm> segments =
      new HashMap<ShmId, RegisteredShm>(0);
  
  private final HashMultimap<ExtendedBlockId, Slot> slots =
      HashMultimap.create(0, 1);
  
  /**
   * 根据配置构造短路读共享内存注册表
   * @param conf Hadoop配置对象
   * @throws IOException 初始化失败时抛出异常
   */
  public ShortCircuitRegistry(Configuration conf) throws IOException {
    boolean enabled = false;
    SharedFileDescriptorFactory shmFactory = null;
    DomainSocketWatcher watcher = null;
    try {
      // 获取域套接字监视器中断检查间隔配置
      int interruptCheck = conf.getInt(
          DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);
      if (interruptCheck <= 0) {
        throw new IOException(
            DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS +
            " was set to " + interruptCheck);
      }
      // 获取共享文件描述符路径配置
      String[] shmPaths =
          conf.getTrimmedStrings(DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS);
      if (shmPaths.length == 0) {
        shmPaths =
            DFS_DATANODE_SHARED_FILE_DESCRIPTOR_PATHS_DEFAULT.split(",");
      }
      // 创建共享文件描述符工厂
      shmFactory = SharedFileDescriptorFactory.
          create("HadoopShortCircuitShm_", shmPaths);
      // 检查域套接字监视器是否加载成功
      String dswLoadingFailure = DomainSocketWatcher.getLoadingFailureReason();
      if (dswLoadingFailure != null) {
        throw new IOException(dswLoadingFailure);
      }
      // 创建域套接字监视器
      watcher = new DomainSocketWatcher(interruptCheck, "datanode");
      enabled = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("created new ShortCircuitRegistry with interruptCheck=" +
                  interruptCheck + ", shmPath=" + shmFactory.getPath());
      }
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disabling ShortCircuitRegistry", e);
      }
    } finally {
      this.enabled = enabled;
      this.shmFactory = shmFactory;
      this.watcher = watcher;
    }
  }

  /**
   * 处理来自FsDatasetCache的块mlock事件，将块对应所有槽位标记为可锚定
   *
   * @param blockId    被mlock的块ID
   */
  public synchronized void processBlockMlockEvent(ExtendedBlockId blockId) {
    if (!enabled) return;
    Set<Slot> affectedSlots = slots.get(blockId);
    for (Slot slot : affectedSlots) {
      slot.makeAnchorable();
    }
  }

  /**
   * 将块对应所有槽位标记为不可锚定，判断是否允许执行munlock
   *
   * @param blockId        块ID
   * @return               如果没有槽位处于锚定状态则返回true，允许munlock；否则返回false
   */
  public synchronized boolean processBlockMunlockRequest(
      ExtendedBlockId blockId) {
    if (!enabled) return true;
    boolean allowMunlock = true;
    Set<Slot> affectedSlots = slots.get(blockId);
    for (Slot slot : affectedSlots) {
      slot.makeUnanchorable();
      if (slot.isAnchored()) {
        allowMunlock = false;
      }
    }
    return allowMunlock;
  }

  /**
   * 处理块失效事件，将块对应所有槽位标记为无效，防止客户端使用该块进行新的短路读操作
   *
   * @param blockId        被删除/失效的块ID
   */
  public synchronized void processBlockInvalidation(ExtendedBlockId blockId) {
    if (!enabled) return;
    final Set<Slot> affectedSlots = slots.get(blockId);
    if (!affectedSlots.isEmpty()) {
      final StringBuilder bld = new StringBuilder();
      String prefix = "";
      bld.append("Block ").append(blockId).append(" has been invalidated.  ").
          append("Marking short-circuit slots as invalid: ");
      for (Slot slot : affectedSlots) {
        slot.makeInvalid();
        bld.append(prefix).append(slot.toString());
        prefix = ", ";
      }
      LOG.info(bld.toString());
    }
  }

  /**
   * 获取访问指定块的所有客户端名称，用逗号拼接返回
   * @param blockId 目标块ID
   * @return 逗号分隔的客户端名称字符串
   */
  public synchronized String getClientNames(ExtendedBlockId blockId) {
    if (!enabled) return "";
    final HashSet<String> clientNames = new HashSet<String>();
    final Set<Slot> affectedSlots = slots.get(blockId);
    for (Slot slot : affectedSlots) {
      clientNames.add(((RegisteredShm)slot.getShm()).getClientName());
    }
    return Joiner.on(",").join(clientNames);
  }

  /**
   * 封装新创建共享内存段的返回信息，支持自动关闭输入流
   */
  public static class NewShmInfo implements Closeable {
    private final ShmId shmId;
    private final FileInputStream stream;

    NewShmInfo(ShmId shmId, FileInputStream stream) {
      this.shmId = shmId;
      this.stream = stream;
    }

    public ShmId getShmId() {
      return shmId;
    }

    public FileInputStream getFileStream() {
      return stream;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /**
   * 处理DFS客户端创建新共享内存段的请求
   *
   * @param clientName    客户端上报的客户端名称
   * @param sock          与该共享内存段关联的域套接字，当套接字关闭时会自动清理该段
   * @return              新共享内存段信息对象，调用方使用后必须关闭该对象
   * @throws IOException  创建共享内存段失败时抛出异常
   */
  public NewShmInfo createNewMemorySegment(String clientName,
      DomainSocket sock) throws IOException {
    NewShmInfo info = null;
    RegisteredShm shm = null;
    ShmId shmId = null;
    synchronized (this) {
      if (!enabled) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("createNewMemorySegment: ShortCircuitRegistry is " +
              "not enabled.");
        }
        throw new UnsupportedOperationException();
      }
      FileInputStream fis = null;
      try {
        // 生成唯一随机ID
        do {
          shmId = ShmId.createRandom();
        } while (segments.containsKey(shmId));
        // 创建共享文件描述符
        fis = shmFactory.createDescriptor(clientName, SHM_LENGTH);
        // 创建已注册共享内存段对象
        shm = new RegisteredShm(clientName, shmId, fis, this);
      } finally {
        if (shm == null) {
          IOUtils.closeStream(fis);
        }
      }
      info = new NewShmInfo(shmId, fis);
      segments.put(shmId, shm);
    }
    // 释放注册表锁避免死锁，之后RegisteredShm#handle随时可能被调用
    watcher.add(sock, shm);
    if (LOG.isTraceEnabled()) {
      LOG.trace("createNewMemorySegment: created " + info.shmId);
    }
    return info;
  }
  
  /**
   * 注册客户端分配的槽位，将槽位关联到指定块
   * @param blockId 目标块ID
   * @param slotId 要注册的槽位ID
   * @param isCached 块是否已被缓存mlock
   * @throws InvalidRequestException 槽位所属共享内存段不存在时抛出异常
   */
  public synchronized void registerSlot(ExtendedBlockId blockId, SlotId slotId,
      boolean isCached) throws InvalidRequestException {
    if (!enabled) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + " can't register a slot because the " +
            "ShortCircuitRegistry is not enabled.");
      }
      throw new UnsupportedOperationException();
    }
    ShmId shmId = slotId.getShmId();
    RegisteredShm shm = segments.get(shmId);
    if (shm == null) {
      throw new InvalidRequestException("there is no shared memory segment " +
          "registered with shmId " + shmId);
    }
    Slot slot = shm.registerSlot(slotId.getSlotIdx(), blockId);
    // 根据缓存状态设置可锚定属性
    if (isCached) {
      slot.makeAnchorable();
    } else {
      slot.makeUnanchorable();
    }
    boolean added = slots.put(blockId, slot);
    Preconditions.checkState(added);
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": registered " + blockId + " with slot " +
        slotId + " (isCached=" + isCached + ")");
    }
  }
  
  /**
   * 注销客户端释放的槽位，清理块和槽位的关联关系
   * @param slotId 要注销的槽位ID
   * @throws InvalidRequestException 槽位所属共享内存段不存在时抛出异常
   */
  public synchronized void unregisterSlot(SlotId slotId)
      throws InvalidRequestException {
    if (!enabled) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("unregisterSlot: ShortCircuitRegistry is " +
            "not enabled.");
      }
      throw new UnsupportedOperationException();
    }
    ShmId shmId = slotId.getShmId();
    RegisteredShm shm = segments.get(shmId);
    if (shm == null) {
      throw new InvalidRequestException("there is no shared memory segment " +
          "registered with shmId " + shmId);
    }
    Slot slot = shm.getSlot(slotId.getSlotIdx());
    slot.makeInvalid();
    shm.unregisterSlot(slotId.getSlotIdx());
    slots.remove(slot.getBlockId(), slot);
  }
  
  /**
   * 关闭注册表，清理所有资源
   */
  public void shutdown() {
    synchronized (this) {
      if (!enabled) return;
      enabled = false;
    }
    IOUtils.closeStream(watcher);
  }

  /**
   * 注册表访问访问接口，用于测试场景遍历内部数据
   */
  public static interface Visitor {
    boolean accept(