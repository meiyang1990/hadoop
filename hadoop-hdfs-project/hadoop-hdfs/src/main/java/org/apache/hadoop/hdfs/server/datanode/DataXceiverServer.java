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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;

/**
 * 文件级注释：HDFS DataNode数据传输服务端，负责监听客户端和其他DataNode的块数据传输请求，
 * 管理数据传输线程和流量控制，不使用Hadoop IPC机制，直接基于TCP处理数据块读写请求。
 *
 * Server used for receiving/sending a block of data. This is created to listen
 * for requests from clients or other DataNodes. This small server does not use
 * the Hadoop IPC mechanism.
 */
class DataXceiverServer implements Runnable {
  public static final Logger LOG = DataNode.LOG;

  /**
   * Default time to wait (in seconds) for the number of running threads to drop
   * below the newly requested maximum before giving up.
   */
  private static final int DEFAULT_RECONFIGURE_WAIT = 30;

  private final PeerServer peerServer;
  private final DataNode datanode;
  private final HashMap<Peer, Thread> peers = new HashMap<>();
  private final HashMap<Peer, DataXceiver> peersXceiver = new HashMap<>();
  private final Lock lock = new ReentrantLock();
  private final Condition noPeers = lock.newCondition();
  private boolean closed = false;
  private int maxReconfigureWaitTime = DEFAULT_RECONFIGURE_WAIT;

  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  volatile int maxXceiverCount;

  /**
   * 块平衡流量限制管理器，用于管控DataNode参与集群负载均衡时的资源使用，
   * 限制并发块移动线程数和总带宽使用，避免均衡操作占用过多资源影响正常业务。
   *
   * A manager to make sure that cluster balancing does not take too much
   * resources.
   *
   * It limits the number of block moves for balancing and the total amount of
   * bandwidth they can use.
   */
  static class BlockBalanceThrottler extends DataTransferThrottler {
    private final Semaphore semaphore;
    private int maxThreads;

   /**
    * 构造方法，初始化块均衡流量控制器
    *
    * @param bandwidth Total amount of bandwidth can be used for balancing
    * @param maxThreads 最大并发块移动线程数
    */
    private BlockBalanceThrottler(long bandwidth, int maxThreads) {
      super(bandwidth);
      this.semaphore = new Semaphore(maxThreads, true);
      this.maxThreads = maxThreads;
      LOG.info("Balancing bandwidth is " + bandwidth + " bytes/s");
      LOG.info("Number threads for balancing is " + maxThreads);
    }

    /**
     * 更新块均衡并发移动线程的最大数量，支持动态扩缩容。
     * 扩容直接生效，缩容需要等待现有线程释放直到满足新上限，超时则失败。
     *
     * @param newMaxThreads The new maximum number of threads for block moving
     * @param duration The number of seconds to wait if decreasing threads
     * @return true if new maximum was successfully applied; false otherwise
     */
    private boolean setMaxConcurrentMovers(final int newMaxThreads,
        final int duration) {
      Preconditions.checkArgument(newMaxThreads > 0);
      final int delta = newMaxThreads - this.maxThreads;
      LOG.debug("Change concurrent thread count to {} from {}", newMaxThreads,
          this.maxThreads);
      if (delta == 0) {
        return true;
      }
      if (delta > 0) {
        LOG.debug("Adding thread capacity: {}", delta);
        this.semaphore.release(delta);
        this.maxThreads = newMaxThreads;
        return true;
      }
      try {
        LOG.debug("Removing thread capacity: {}. Max wait: {}", delta,
            duration);
        boolean acquired = this.semaphore.tryAcquire(Math.abs(delta), duration,
            TimeUnit.SECONDS);
        if (acquired) {
          this.maxThreads = newMaxThreads;
        } else {
          LOG.warn("Could not lower thread count to {} from {}. Too busy.",
              newMaxThreads, this.maxThreads);
        }
        return acquired;
      } catch (InterruptedException e) {
        LOG.warn("Interrupted before adjusting thread count: {}", delta);
        return false;
      }
    }

    @VisibleForTesting
    int getMaxConcurrentMovers() {
      return this.maxThreads;
    }

   /**
    * 申请块移动的线程配额，成功则可开始移动，失败则拒绝本次移动。
    *
    * Return true if the thread quota is not exceeded and
    * the counter is incremented; False otherwise.
    */
    boolean acquire() {
      return this.semaphore.tryAcquire();
    }

    /**
     * 释放块移动的线程配额，移动完成后调用。
     * Mark that the move is completed. The thread counter is decremented.
     */
    void release() {
      this.semaphore.release();
    }
  }

  final BlockBalanceThrottler balanceThrottler;

  private volatile DataTransferThrottler transferThrottler;

  private volatile DataTransferThrottler writeThrottler;

  private volatile DataTransferThrottler readThrottler;

  /**
   * 预估块大小，用于磁盘空间检查。旧客户端不传递预期块大小时，使用服务端默认块大小。
   * Stores an estimate for block size to check if the disk partition has enough
   * space. Newer clients pass the expected block size to the DataNode. For
   * older clients, just use the server-side default block size.
   */
  final long estimateBlockSize;

  /**
   * 构造DataXceiver服务端，从配置中初始化各类参数和限流工具。
   *
   * @param peerServer 对等连接服务端，负责监听TCP连接
   * @param conf Hadoop配置对象
   * @param datanode 所属DataNode实例
   */
  DataXceiverServer(PeerServer peerServer, Configuration conf,
      DataNode datanode) {
    this.peerServer = peerServer;
    this.datanode = datanode;

    this.maxXceiverCount =
      conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                  DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);
    Preconditions.checkArgument(this.maxXceiverCount >= 1,
        DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY +
        " should not be less than 1.");

    this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

    // 初始化集群均衡相关参数
    this.balanceThrottler = new BlockBalanceThrottler(
        conf.getLongBytes(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT));
    initBandwidthPerSec(conf);
  }

  /**
   * 从配置初始化整体、读、写三个维度的数据传输限流工具。
   */
  private void initBandwidthPerSec(Configuration conf) {
    long bandwidthPerSec = conf.getLongBytes(
        DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT);
    if (bandwidthPerSec > 0) {
      this.transferThrottler = new DataTransferThrottler(bandwidthPerSec);
    } else {
      this.transferThrottler = null;
    }

    bandwidthPerSec = conf.getLongBytes(
        DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT);
    if (bandwidthPerSec > 0) {
      this.writeThrottler = new DataTransferThrottler(bandwidthPerSec);
    } else {
      this.writeThrottler = null;
    }

    bandwidthPerSec = conf.getLongBytes(
        DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY,
        DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_DEFAULT);
    if (bandwidthPerSec > 0) {
      this.readThrottler = new DataTransferThrottler(bandwidthPerSec);
    } else {
      this.readThrottler = null;
    }
  }

  @Override
  public void run() {
    Peer peer = null;
    while (datanode.shouldRun && !datanode.shutdownForUpgrade) {
      try {
        // 接受新的客户端连接
        peer = peerServer.accept();

        // 检查并发传输线程数是否超过上限
        int curXceiverCount = datanode.getXceiverCount();
        if (curXceiverCount > maxXceiverCount) {
          throw new IOException("Xceiver count " + curXceiverCount
              + " exceeds the limit of concurrent xceivers: "
              + maxXceiverCount);
        }

        // 启动新的DataXceiver守护线程处理本次传输请求
        new Daemon(datanode.threadGroup,
            DataXceiver.create(peer, datanode, this))
            .start();
      } catch (SocketTimeoutException ignored) {
        // 超时后重新循环，检查是否需要继续运行
      } catch (AsynchronousCloseException ace) {
        // 关闭监听套接字时会触发该异常，仅在非关闭场景下打印警告
        if (datanode.shouldRun && !datanode.shutdownForUpgrade) {
          LOG.warn("{}:DataXceiverServer", datanode.getDisplayName(), ace);
        }
      } catch (IOException ie) {
        IOUtils.closeStream(peer);
        LOG.warn("{}:DataXceiverServer", datanode.getDisplayName(), ie);
      } catch (OutOfMemoryError ie) {
        IOUtils.closeStream(peer);
        // 并发过高导致OOM时，日志记录后休眠30秒等待现有传输完成
        LOG.error("DataNode is out of memory. Will retry in 30 seconds.", ie);
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(30L));
        } catch (InterruptedException e) {
          // ignore
        }
      } catch (Throwable te) {
        LOG.error("{}:DataXceiverServer: Exiting.", datanode.getDisplayName(),
            te);
        datanode.shouldRun = false;
      }
    }

    // 关闭服务端，停止接受新请求
    lock.lock();
    try {
      if (!closed) {
        peerServer.close();
        closed = true;
      }
    } catch (IOException ie) {
      LOG.warn("{}:DataXceiverServer: close exception",
          datanode.getDisplayName(), ie);
    } finally {
      lock.unlock();
    }

    // 升级重启场景，通知所有连接后等待关闭
    if (datanode.shutdownForUpgrade) {
      restartNotifyPeers();
      // 等待现有连接处理完成，强制关闭超时未退出的线程
      LOG.info("Shutting down DataXceiverServer before restart");

      waitAllPeers(2L, TimeUnit.SECONDS);
    }

    // 关闭所有活动连接
    closeAllPeers();
  }

  /**
   * 强制关闭DataXceiver服务，用于DataNode关机流程。
   */
  void kill() {
    assert (datanode.shouldRun == false || datanode.shutdownForUpgrade) :
      "shoudRun should be set to false or restarting should be true"
      + " before killing";
    lock.lock();
    try {
      if (!closed) {
        peerServer.close();
        closed = true;
      }
    } catch (IOException ie) {
      LOG.warn("{}:DataXceiverServer.kill()", datanode.getDisplayName(), ie);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 添加新的活动连接到管理集合，并发控制通过锁保证线程安全。
   *
   * @param peer 客户端对等连接
   * @param t 处理该连接的线程
   * @param xceiver 处理该连接的DataXceiver实例
   * @throws IOException 服务已关闭时抛出异常
   */
  void addPeer(Peer peer, Thread t, DataXceiver xceiver)
      throws IOException {
    lock.lock();
    try {
      if (closed) {
        throw new IOException("Server closed.");
      }
      peers.put(peer, t);
      peersXceiver.put(peer, xceiver);
      datanode.metrics.incrDataNodeActiveXceiversCount();
    } finally {
      lock.unlock();
    }
  }

  /**
   * 关闭并移除指定连接，更新指标计数。
   *
   * @param peer 要关闭的对等连接
   */
  void closePeer(Peer peer) {
    lock.lock();
    try {
      peers.remove(peer);
      peersXceiver.remove(peer);
      datanode.metrics.decrDataNodeActiveXceiversCount();
      IOUtils.closeStream(peer);
      if (peers.isEmpty()) {
        this.noPeers.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * 向所有活动连接发送OOB（带外）消息，用于升级重启场景通知客户端断开重连。
   */
  // Sending OOB to all peers
  public void sendOOBToPeers() {
    lock.lock();
    try {
      if (!datanode.shutdownForUpgrade) {
        return;
      }
      for (Peer p : peers.keySet()) {
        try {
          peersXceiver.get(p).sendOOB();
        } catch (IOException e) {
          LOG.warn("Got error when sending OOB message.", e);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted when sending OOB message.");
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * 停止所有连接上的写操作，用于优雅关闭流程。
   */
  public void stopWriters() {
    lock.lock();
    try {
      peers.keySet().forEach(p -> peersXceiver.get(p).stopWriter());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Notify all peers of the shutdown and restart. 'datanode.shouldRun' should
   * still be true and 'datanode.restarting' should be set true before calling
   * this method.
   */
  void restartNotifyPeers() {
    assert (datanode.shouldRun && datanode.shutdownForUpgrade);
    lock.lock();
    try {
      // 中断所有DataXceiver处理线程，通知重启
      peers.values().forEach(t -> t.interrupt());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Close all peers and clear the map.
   */
  void closeAllPeers() {
    LOG.info("Closing all peers.");
    lock.lock();
    try {
      // 关闭所有活动连接
      peers.keySet().forEach(IOUtils::closeStream);
      peers.clear();
      peersXceiver.clear();
      // 重置指标计数
      datanode.metrics.setDataNodeActiveXceiversCount(0);
      datanode.metrics.setData