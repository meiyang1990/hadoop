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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.CheckpointFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SaveNamespaceCancelledException;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  standby状态NameNode中的检查点线程，负责定期对命名空间生成检查点
 *  生成检查点后保存到本地存储，并上传到所有对端Active NameNode
 *  用于HDFS高可用架构中，由Standby节点负责生成检查点，分担Active节点的压力
 */
@InterfaceAudience.Private
public class StandbyCheckpointer {
  private static final Logger LOG =
      LoggerFactory.getLogger(StandbyCheckpointer.class);
  // 取消检查点后，禁止新检查点启动的时间长度（2分钟）
  private static final long PREVENT_AFTER_CANCEL_MS = 2*60*1000L;
  private final CheckpointConf checkpointConf;
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private long lastCheckpointTime;
  private final CheckpointerThread thread;
  private final ThreadFactory uploadThreadFactory;
  // 所有对端Active NameNode的HTTP地址列表
  private List<URL> activeNNAddresses;
  // 当前Standby NameNode自身的HTTP地址
  private URL myNNAddress;

  private final Object cancelLock = new Object();
  private Canceler canceler;

  // 统计被取消的检查点数量，仅用于测试
  private static int canceledCount = 0;

  // NameNode地址到最近上传记录的映射，记录每个对端节点的上传状态
  private final HashMap<String, CheckpointReceiverEntry> checkpointReceivers;
  
  /**
   * 构造Standby检查点管理器，初始化配置和线程
   * @param conf Hadoop配置对象
   * @param ns 当前Standby节点的命名系统对象
   * @throws IOException 如果地址解析失败则抛出异常
   */
  public StandbyCheckpointer(Configuration conf, FSNamesystem ns)
      throws IOException {
    this.namesystem = ns;
    this.conf = conf;
    this.checkpointConf = new CheckpointConf(conf); 
    this.thread = new CheckpointerThread();
    this.uploadThreadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("TransferFsImageUpload-%d").build();
    setNameNodeAddresses(conf);
    this.checkpointReceivers = new HashMap<>();
    for (URL address : activeNNAddresses) {
      this.checkpointReceivers.put(address.toString(),
          new CheckpointReceiverEntry());
    }
  }

  /**
   * 内部类，存储每个对端NameNode的检查点接收状态
   */
  private static final class CheckpointReceiverEntry {
    // 最近一次成功上传的时间
    private long lastUploadTime;
    // 当前Standby是否是该对端节点的主检查点提供者
    private boolean isPrimary;

    CheckpointReceiverEntry() {
      this.lastUploadTime = 0L;
      this.isPrimary = true;
    }

    void setLastUploadTime(long lastUploadTime) {
      this.lastUploadTime = lastUploadTime;
    }

    void setIsPrimary(boolean isPrimaryFor) {
      this.isPrimary = isPrimaryFor;
    }

    long getLastUploadTime() {
      return lastUploadTime;
    }

    boolean isPrimary() {
      return isPrimary;
    }
  }

  /**
   * 从配置中解析当前节点和对端Active节点的HTTP地址
   * @throws IOException 如果地址解析或校验失败则抛出异常
   */
  private void setNameNodeAddresses(Configuration conf) throws IOException {
    // 获取当前Standby节点的HTTP地址
    myNNAddress = getHttpAddress(conf);

    // 获取所有其他NameNode的配置
    List<Configuration> confForActive = HAUtil.getConfForOtherNodes(conf);
    activeNNAddresses = new ArrayList<URL>(confForActive.size());
    for (Configuration activeConf : confForActive) {
      URL activeNNAddress = getHttpAddress(activeConf);

      // 对每个Active地址做合法性校验
      Preconditions.checkArgument(checkAddress(activeNNAddress),
          "Bad address for active NN: %s", activeNNAddress);

      activeNNAddresses.add(activeNNAddress);
    }

    // 校验当前Standby地址合法性
    Preconditions.checkArgument(checkAddress(myNNAddress), "Bad address for standby NN: %s",
        myNNAddress);
  }
  
  /**
   * 从配置中解析NameNode的HTTP服务地址
   * @param conf 对应NameNode的配置对象
   * @return 解析后的HTTP地址URL对象
   * @throws IOException 如果地址格式错误则抛出异常
   */
  private URL getHttpAddress(Configuration conf) throws IOException {
    final String scheme = DFSUtil.getHttpClientScheme(conf);
    String defaultHost = NameNode.getServiceAddress(conf, true).getHostName();
    URI addr = DFSUtil.getInfoServerWithDefaultHost(defaultHost, conf, scheme);
    return addr.toURL();
  }
  
  /**
   * 校验地址是否合法（必须指定非零端口）
   * @param addr 需要校验的URL地址
   * @return 合法返回true，否则返回false
   */
  private static boolean checkAddress(URL addr) {
    return addr.getPort() != 0;
  }

  /**
   * 启动检查点后台线程
   */
  public void start() {
    LOG.info("Starting standby checkpoint thread...\n" +
        "Checkpointing active NN to possible NNs: {}\n" +
        "Serving checkpoints at {}", activeNNAddresses, myNNAddress);
    thread.start();
  }
  
  /**
   * 停止检查点线程，取消正在进行的检查点
   * @throws IOException 如果线程等待退出被中断则抛出异常
   */
  public void stop() throws IOException {
    cancelAndPreventCheckpoints("Stopping checkpointer");
    thread.setShouldRun(false);
    thread.interrupt();
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }

  /**
   * 触发回滚检查点操作，中断当前休眠唤醒检查
   */
  public void triggerRollbackCheckpoint() {
    thread.interrupt();
  }

  /**
   * 执行一次完整的检查点流程：保存命名空间到本地，然后上传到所有对端NameNode
   * @throws InterruptedException 如果检查点被中断则抛出异常
   * @throws IOException 如果IO操作失败则抛出异常
   */
  private void doCheckpoint() throws InterruptedException, IOException {
    assert canceler != null;
    final long txid;
    final NameNodeFile imageType;
    // 获取检查点锁，防止和编辑日志重放冲突
    namesystem.cpLockInterruptibly();
    try {
      assert namesystem.getEditLog().isOpenForRead() :
        "Standby Checkpointer should only attempt a checkpoint when " +
        "NN is in standby mode, but the edit logs are in an unexpected state";

      FSImage img = namesystem.getFSImage();

      long prevCheckpointTxId = img.getStorage().getMostRecentCheckpointTxId();
      long thisCheckpointTxId = img.getCorrectLastAppliedOrWrittenTxId();
      assert thisCheckpointTxId >= prevCheckpointTxId;
      // 没有新事务，跳过本次检查点
      if (thisCheckpointTxId == prevCheckpointTxId) {
        LOG.info("A checkpoint was triggered but the Standby Node has not " +
            "received any transactions since the last checkpoint at txid {}. " +
            "Skipping...", thisCheckpointTxId);
        return;
      }

      // 判断是否需要生成回滚检查点（用于滚动升级场景）
      if (namesystem.isRollingUpgrade()
          && !namesystem.getFSImage().hasRollbackFSImage()) {
        // 滚动升级且未生成回滚镜像时，将本次检查点标记为回滚镜像
        imageType = NameNodeFile.IMAGE_ROLLBACK;
      } else {
        imageType = NameNodeFile.IMAGE;
      }
      // 保存命名空间生成检查点
      img.saveNamespace(namesystem, imageType, canceler);
      txid = img.getStorage().getMostRecentCheckpointTxId();
      assert txid == thisCheckpointTxId : "expected to save checkpoint at txid=" +
          thisCheckpointTxId + " but instead saved at txid=" + txid;

      // 如果配置了离线镜像查看工具输出目录，则保存旧版本格式镜像
      String outputDir = checkpointConf.getLegacyOivImageDir();
      if (outputDir != null && !outputDir.isEmpty()) {
        try {
          img.saveLegacyOIVImage(namesystem, outputDir, canceler);
        } catch (IOException ioe) {
          LOG.warn("Exception encountered while saving legacy OIV image; "
                  + "continuing with other checkpointing steps", ioe);
        }
      }
    } finally {
      // 释放检查点锁
      namesystem.cpUnlock();
    }

    // 启动线程池并行上传检查点到所有对端Active节点，避免阻塞切主流程（参考HDFS-4816）
    int poolSize = checkpointConf.isParallelUploadEnabled() ? activeNNAddresses.size() : 0;
    ExecutorService executor = new ThreadPoolExecutor(poolSize, activeNNAddresses.size(), 100,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(activeNNAddresses.size()),
        uploadThreadFactory);
    HashMap<String, Future<TransferFsImage.TransferResult>> uploads =
        new HashMap<>();
    for (final URL activeNNAddress : activeNNAddresses) {
      // 满足以下任一条件才上传：
      // 1. 当前Standby是该节点的主检查点提供者
      // 2. 距离上次上传已经超过静默期
      String addressString = activeNNAddress.toString();
      assert checkpointReceivers.containsKey(addressString);
      CheckpointReceiverEntry receiverEntry =
          checkpointReceivers.get(addressString);
      long secsSinceLastUpload =
          TimeUnit.MILLISECONDS.toSeconds(
              monotonicNow() - receiverEntry.getLastUploadTime());
      boolean shouldUpload = receiverEntry.isPrimary() ||
          secsSinceLastUpload >= checkpointConf.getQuietPeriod();
      if (shouldUpload) {
        Future<TransferFsImage.TransferResult> upload =
            executor.submit(new Callable<TransferFsImage.TransferResult>() {
              @Override
              public TransferFsImage.TransferResult call()
                  throws IOException, InterruptedException {
                CheckpointFaultInjector.getInstance().duringUploadInProgess();
                // 从本地存储上传镜像到对端NameNode
                return TransferFsImage.uploadImageFromStorage(activeNNAddress,
                    conf, namesystem.getFSImage().getStorage(), imageType, txid,
                    canceler);
              }
            });
        uploads.put(addressString, upload);
      }
    }
    InterruptedException ie = null;
    List<IOException> ioes = Lists.newArrayList();
    // 收集所有上传任务的结果
    for (Map.Entry<String, Future<TransferFsImage.TransferResult>> entry :
        uploads.entrySet()) {
      String url = entry.getKey();
      Future<TransferFsImage.TransferResult> upload = entry.getValue();
      try {
        CheckpointReceiverEntry receiverEntry = checkpointReceivers.get(url);
        TransferFsImage.TransferResult uploadResult = upload.get();
        if (uploadResult == TransferFsImage.TransferResult.SUCCESS) {
          // 上传成功，更新最近上传时间和主检查点标记
          receiverEntry.setLastUploadTime(monotonicNow());
          receiverEntry.setIsPrimary(true);
        } else {
          // 上传被对端拒绝，降级为备检查点提供者
          LOG.info("Image upload rejected by the other NameNode: {}",
              uploadResult);
          receiverEntry.setIsPrimary(false);
        }
      } catch (ExecutionException e) {
        // 上传异常，记录错误，继续处理其他节点
        ioes.add(new IOException("Exception during image upload", e));
      } catch (InterruptedException e) {
        ie = e;
        break;
      }
    }
    // 处理中断异常
    if (ie != null) {
      // 取消所有剩余上传任务
      for (Map.Entry<String, Future<TransferFsImage.TransferResult>> entry :
          uploads.entrySet()) {
        Future<TransferFsImage.TransferResult> upload = entry.getValue();
        upload.cancel(true);
      }

      // 关闭线程池
      executor.shutdownNow();
      executor.awaitTermination(500, TimeUnit.MILLISECONDS);

      throw ie;
    }

    // 超过一半节点上传失败时，抛出聚合异常
    if (ioes.size() > activeNNAddresses.size() / 2) {
      throw MultipleIOException.createIOException(ioes);
    }
  }
  
  /**
   * 取消当前正在进行的检查点，并在指定时间内禁止新检查点启动
   * 用于故障转移准备阶段，避免检查点和切主操作冲突
   * @param msg 取消原因描述
   * @throws ServiceFailedException 服务失败异常
   */
  public void cancelAndPreventCheckpoints(String msg) throws ServiceFailedException {
    synchronized (cancelLock) {
      thread.preventCheckpointsFor(PREVENT_AFTER_CANCEL_MS);

      // 如果检查点已经开始进行，取消它
      if (canceler != null) {
        canceler.cancel(msg);
      }
    }
  }
  
  @VisibleForTesting
  static int getCanceledCount() {
    return canceledCount;
  }

  @VisibleForTesting
  public long getLastCheckpointTime() {
    return lastCheckpointTime;
  }

  /**
   * 计算自上次检查点以来新增的未检查点事务数量
   * @return 未检查点事务数
   */
  private long countUncheckpointedTxns() {
    FSImage img = namesystem.getFSImage();
    return img.getCorrectLastAppliedOrWrittenTxId() -
      img.getStorage().getMostRecentCheckpointTxId();
  }

  /**
   * 后台检查点线程，周期性触发检查点操作
   */
  private class CheckpointerThread extends SubjectInheritingThread {
    private volatile boolean shouldRun = true;
    // 禁止检查点的截止时间戳
    private volatile long preventCheckpointsUntil = 0;

    private CheckpointerThread() {
      super("Standby State Checkpointer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }

    @Override
    public void work() {
      // 使用登录用户身份执行，保证Kerberos认证正常
      SecurityUtil.doAsLoginUserOrFatal(
          new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            doWork();
            return null;
          }
        });
    }

    /**
     * 设置禁止检查点的时间段，用于切主前阻止新检查点启动
     * @param delayMs