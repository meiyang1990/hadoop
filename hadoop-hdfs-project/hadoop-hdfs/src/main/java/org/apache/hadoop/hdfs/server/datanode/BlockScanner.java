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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.VolumeScanner.ScanResultHandler;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Datanode块扫描管理器，管理每个存储卷对应的卷扫描器，负责定期校验本节点存储的所有数据块校验和，
 * 及时发现坏块并上报NameNode进行处理，保障HDFS数据可靠性。核心职责包括：扫描器生命周期管理、
 * 块池扫描权限控制、可疑块标记、对外提供扫描统计信息。
 */
@InterfaceAudience.Private
public class BlockScanner {
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockScanner.class);

  /**
   * 关联的DataNode实例
   */
  private final DataNode datanode;

  /**
   * 存储ID -> 对应卷扫描器实例的映射表
   */
  private final TreeMap<String, VolumeScanner> scanners =
      new TreeMap<String, VolumeScanner>();

  /**
   * 块扫描器配置
   */
  private Conf conf;

  /**
   * 移除所有卷扫描器时，等待VolumeScanner停止的超时时间（毫秒）
   */
  private long joinVolumeScannersTimeOutMs;

  @VisibleForTesting
  void setConf(Conf conf) {
    this.conf = conf;
    // 更新所有已存在卷扫描器的配置
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      entry.getValue().setConf(conf);
    }
  }

  /**
   * 块扫描器配置类，封装从Hadoop配置加载的所有块扫描相关参数，支持单元测试自定义配置覆盖。
   */
  static class Conf {
    // 以下为单元测试使用的内部配置键，仅当allowUnitTestSettings为true时生效

    @VisibleForTesting
    static final String INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS =
        "internal.dfs.datanode.scan.period.ms.key";

    @VisibleForTesting
    static final String INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER =
        "internal.volume.scanner.scan.result.handler";

    @VisibleForTesting
    static final String INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS =
        "internal.dfs.block.scanner.max_staleness.ms";

    @VisibleForTesting
    static final long INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS_DEFAULT =
        TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

    @VisibleForTesting
    static final String INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS =
        "dfs.block.scanner.cursor.save.interval.ms";

    @VisibleForTesting
    static final long
        INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS_DEFAULT =
            TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    /** 是否允许单元测试修改配置，生产环境默认关闭 */
    static boolean allowUnitTestSettings = false;
    /** 单个存储卷每秒允许扫描的字节数，控制扫描IO带宽占用 */
    final long targetBytesPerSec;
    /** 扫描记录最大过期时间，超过该时间未扫描的块会被优先扫描 */
    final long maxStalenessMs;
    /** 完成整个存储卷所有块扫描的周期，单位毫秒 */
    final long scanPeriodMs;
    /** 保存扫描游标到磁盘的间隔，单位毫秒 */
    final long cursorSaveMs;
    /** 是否跳过最近被访问过的块，减少扫描对业务IO的影响 */
    final boolean skipRecentAccessed;
    /** 扫描结果处理器类，用于处理扫描发现的问题块 */
    final Class<? extends ScanResultHandler> resultHandler;

    /**
     * 单元测试环境获取自定义配置参数，生产环境返回默认值
     */
    private static long getUnitTestLong(Configuration conf, String key,
                                        long defVal) {
      if (allowUnitTestSettings) {
        return conf.getLong(key, defVal);
      } else {
        return defVal;
      }
    }

    /**
     * 从配置中解析块扫描周期，处理向下兼容逻辑：
     * 配置为0时使用默认3周周期；配置小于0时禁用扫描
     * @param conf Hadoop配置对象
     * @return 块扫描周期，单位毫秒
     */
    private static long getConfiguredScanPeriodMs(Configuration conf) {
      long tempScanPeriodMs = getUnitTestLong(
          conf, INTERNAL_DFS_DATANODE_SCAN_PERIOD_MS,
              TimeUnit.MILLISECONDS.convert(conf.getLong(
                  DFS_DATANODE_SCAN_PERIOD_HOURS_KEY,
                  DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT), TimeUnit.HOURS));

      if (tempScanPeriodMs == 0) {
        tempScanPeriodMs = TimeUnit.MILLISECONDS.convert(
            DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT, TimeUnit.HOURS);
      }

      return tempScanPeriodMs;
    }

    @SuppressWarnings("unchecked")
    Conf(Configuration conf) {
      // 加载并校验每秒扫描字节数配置，不能为负
      this.targetBytesPerSec = Math.max(0L, conf.getLong(
          DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND,
          DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND_DEFAULT));
      // 加载最大过期时间配置
      this.maxStalenessMs = Math.max(0L, getUnitTestLong(conf,
          INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS,
          INTERNAL_DFS_BLOCK_SCANNER_MAX_STALENESS_MS_DEFAULT));
      // 解析扫描周期
      this.scanPeriodMs = getConfiguredScanPeriodMs(conf);
      // 加载游标保存间隔配置
      this.cursorSaveMs = Math.max(0L, getUnitTestLong(conf,
          INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS,
          INTERNAL_DFS_BLOCK_SCANNER_CURSOR_SAVE_INTERVAL_MS_DEFAULT));
      // 加载是否跳过最近访问块配置
      this.skipRecentAccessed = conf.getBoolean(
          DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED,
          DFS_BLOCK_SCANNER_SKIP_RECENT_ACCESSED_DEFAULT);
      // 仅单元测试允许自定义结果处理器
      if (allowUnitTestSettings) {
        this.resultHandler = (Class<? extends ScanResultHandler>)
            conf.getClass(INTERNAL_VOLUME_SCANNER_SCAN_RESULT_HANDLER,
                          ScanResultHandler.class);
      } else {
        this.resultHandler = ScanResultHandler.class;
      }
    }
  }

  /**
   * 构造块扫描器，使用DataNode自身配置初始化
   * @param datanode 关联的DataNode实例
   */
  public BlockScanner(DataNode datanode) {
    this(datanode, datanode.getConf());
  }

  /**
   * 构造块扫描器，使用指定配置初始化
   * @param datanode 关联的DataNode实例
   * @param conf Hadoop配置对象
   */
  public BlockScanner(DataNode datanode, Configuration conf) {
    this.datanode = datanode;
    // 加载卷扫描器停止超时配置
    setJoinVolumeScannersTimeOutMs(
        conf.getLong(DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_KEY,
            DFS_BLOCK_SCANNER_VOLUME_JOIN_TIMEOUT_MSEC_DEFAULT));
    this.conf = new Conf(conf);
    // 打印初始化日志
    if (isEnabled()) {
      LOG.info("Initialized block scanner with targetBytesPerSec {}",
          this.conf.targetBytesPerSec);
    } else {
      LOG.info("Disabled block scanner.");
    }
  }

  /**
   * 检查块扫描器是否启用，扫描周期和带宽都大于0才启用
   * @return true表示启用，false表示禁用
   */
  public boolean isEnabled() {
    return (conf.scanPeriodMs > 0) && (conf.targetBytesPerSec > 0);
  }

  /**
   * 检查当前是否注册了任何卷扫描器
   * @return true表示至少有一个注册的扫描器
   */
  public synchronized boolean hasAnyRegisteredScanner() {
    return !scanners.isEmpty();
  }

 /**
  * 为指定存储卷添加卷扫描器
  * @param ref 存储卷引用
  */
  public synchronized void addVolumeScanner(FsVolumeReference ref) {
    boolean success = false;
    try {
      FsVolumeSpi volume = ref.getVolume();
      // 扫描器未启用直接返回
      if (!isEnabled()) {
        LOG.debug("Not adding volume scanner for {}, because the block " +
            "scanner is disabled.", volume);
        return;
      }
      // 已存在对应扫描器，打印错误日志返回
      VolumeScanner scanner = scanners.get(volume.getStorageID());
      if (scanner != null) {
        LOG.error("Already have a scanner for volume {}.",
            volume);
        return;
      }
      LOG.debug("Adding scanner for volume {} (StorageID {})",
          volume, volume.getStorageID());
      // 创建并启动新的卷扫描器，注册到映射表
      scanner = new VolumeScanner(conf, datanode, ref);
      scanner.start();
      scanners.put(volume.getStorageID(), scanner);
      success = true;
    } finally {
      // 创建失败，释放存储卷引用
      if (!success) {
        IOUtils.cleanupWithLogger(null, ref);
      }
    }
  }

  /**
   * 停止并移除指定存储卷的卷扫描器，会阻塞等待扫描器线程退出
   * @param volume 要移除扫描器的存储卷
   */
  public synchronized void removeVolumeScanner(FsVolumeSpi volume) {
    if (!isEnabled()) {
      LOG.debug("Not removing volume scanner for {}, because the block " +
          "scanner is disabled.", volume.getStorageID());
      return;
    }
    VolumeScanner scanner = scanners.get(volume.getStorageID());
    if (scanner == null) {
      LOG.warn("No scanner found to remove for volumeId {}",
          volume.getStorageID());
      return;
    }
    LOG.info("Removing scanner for volume {} (StorageID {})",
        volume, volume.getStorageID());
    // 关闭扫描器并从映射表移除，阻塞等待线程退出
    scanner.shutdown();
    scanners.remove(volume.getStorageID());
    Uninterruptibles.joinUninterruptibly(scanner, 5, TimeUnit.MINUTES);
  }

  /**
   * 停止并移除所有卷扫描器，用于DataNode关闭流程。即使部分扫描器超时未退出也会返回，
   * 不阻塞关机流程，扫描器线程作为守护线程不会影响进程退出。
   */
  public synchronized void removeAllVolumeScanners() {
    // 先通知所有扫描器关闭
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      entry.getValue().shutdown();
    }
    // 依次等待每个扫描器线程退出，超时后继续处理
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      Uninterruptibles.joinUninterruptibly(entry.getValue(),
          getJoinVolumeScannersTimeOutMs(), TimeUnit.MILLISECONDS);
    }
    // 清空扫描器映射表
    scanners.clear();
  }

  /**
   * 允许对指定块池ID的块进行扫描
   * @param bpid 块池ID
   */
  synchronized void enableBlockPoolId(String bpid) {
    Preconditions.checkNotNull(bpid);
    // 通知所有卷扫描器启用该块池扫描
    for (VolumeScanner scanner : scanners.values()) {
      scanner.enableBlockPoolId(bpid);
    }
  }

  /**
   * 禁止对指定块池ID的块进行扫描
   * @param bpid 块池ID
   */
  synchronized void disableBlockPoolId(String bpid) {
    Preconditions.checkNotNull(bpid);
    // 通知所有卷扫描器禁用该块池扫描
    for (VolumeScanner scanner : scanners.values()) {
      scanner.disableBlockPoolId(bpid);
    }
  }

  @VisibleForTesting
  synchronized VolumeScanner.Statistics getVolumeStats(String volumeId) {
    VolumeScanner scanner = scanners.get(volumeId);
    if (scanner == null) {
      return null;
    }
    return scanner.getStatistics();
  }

  /**
   * 将所有卷扫描器的统计信息拼接输出到StringBuilder
   * @param p 用于接收统计信息的StringBuilder
   */
  synchronized void printStats(StringBuilder p) {
    // 拼接每个卷扫描器的统计信息
    for (Entry<String, VolumeScanner> entry : scanners.entrySet()) {
      entry.getValue().printStats(p);
    }
  }

  /**
   * 将指定块标记为可疑块，会安排该块尽快被重新扫描校验。用于IO错误时主动触发校验，确认块是否损坏。
   * 内部会对短时间内重复标记的同一块去重，避免重复扫描浪费资源。
   * @param storageId 块所在存储ID
   * @param block 待标记的块信息
   */
  synchronized void markSuspectBlock(String storageId, ExtendedBlock block) {
    if (!isEnabled()) {
      LOG.debug("Not scanning suspicious block {} on {}, because the block " +
          "scanner is disabled.", block, storageId);
      return;
    }
    VolumeScanner scanner = scanners.get(storageId);
    if (scanner == null) {
      // 存储卷正在被移除时可能出现该情况，属于正常场景
      LOG.info("Not scanning suspicious block {} on {}, because there is no " +
          "volume scanner for that storageId.", block, storageId);
      return;
    }
    // 委托对应卷扫描器标记可疑块
    scanner.markSuspectBlock(block);
  }

  public long getJoinVolumeScannersTimeOutMs() {
    return joinVolumeScannersTimeOutMs;
  }

  public void setJoinVolumeScannersTimeOutMs(long joinScannersTimeOutMs) {
    this.joinVolumeScannersTimeOutMs = joinScannersTimeOutMs;
  }

  /**
   * BlockScanner的HTTP统计信息Servlet，响应DataNode Web UI的块扫描统计查询请求，返回所有卷扫描器的统计信息。
   */
  @InterfaceAudience.Private
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) throws IOException {
      // 设置响应类型为纯文本
      response.setContentType("text/plain");

      // 从Servlet上下文获取关联的DataNode和BlockScanner
      DataNode datanode = (DataNode)
          getServletContext().getAttribute