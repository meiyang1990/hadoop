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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;


import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MountVolumeMap;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HDFS DataNode 块存储层的服务提供者接口，定义了底层存储管理数据块副本的核心抽象
 * 默认实现将块副本存储在本地磁盘，支持扩展自定义存储实现
 */
@InterfaceAudience.Private
public interface FsDatasetSpi<V extends FsVolumeSpi> extends FSDatasetMBean {
  /**
   * FsDatasetSpi 对象的工厂抽象类，用于创建不同实现的数据集对象
   * @param <D> 创建的数据集类型
   */
  abstract class Factory<D extends FsDatasetSpi<?>> {
    /**
     * 根据配置获取工厂实例
     * @param conf Hadoop配置对象
     * @return 配置指定的FsDataset工厂实例
     */
    public static Factory<?> getFactory(Configuration conf) {
      @SuppressWarnings("rawtypes")
      final Class<? extends Factory> clazz = conf.getClass(
          DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
          FsDatasetFactory.class,
          Factory.class);
      return ReflectionUtils.newInstance(clazz, conf);
    }

    /**
     * 创建一个新的FsDataset实例
     * @param datanode DataNode实例
     * @param storage DataNode存储管理对象
     * @param conf Hadoop配置对象
     * @return 新创建的FsDataset实例
     * @throws IOException 创建失败时抛出IO异常
     */
    public abstract D newInstance(DataNode datanode, DataStorage storage,
        Configuration conf) throws IOException;

    /**
     * 判断当前工厂是否创建模拟测试对象
     * @return 如果是模拟实现返回true，否则返回false
     */
    public boolean isSimulated() {
      return false;
    }
  }

  /**
   * 不可修改的FsVolume引用列表，维护所有卷的引用计数
   * 调用者必须在使用完毕后调用close方法释放所有引用计数
   */
  class FsVolumeReferences implements Iterable<FsVolumeSpi>, Closeable {
    private final List<FsVolumeReference> references;

    /**
     * 构造方法，为每个卷获取引用并维护
     * @param curVolumes 当前所有可用卷列表
     */
    public <S extends FsVolumeSpi> FsVolumeReferences(List<S> curVolumes) {
      references = new ArrayList<>();
      // 遍历所有卷，尝试获取引用，忽略已经关闭的卷
      for (FsVolumeSpi v : curVolumes) {
        try {
          references.add(v.obtainReference());
        } catch (ClosedChannelException e) {
          // 该卷已关闭，直接忽略
        }
      }
    }

    /**
     * FsVolumeReferences的迭代器实现
     */
    private static class FsVolumeSpiIterator implements
        Iterator<FsVolumeSpi> {
      private final List<FsVolumeReference> references;
      private int idx = 0;

      FsVolumeSpiIterator(List<FsVolumeReference> refs) {
        references = refs;
      }

      @Override
      public boolean hasNext() {
        return idx < references.size();
      }

      @Override
      public FsVolumeSpi next() {
        int refIdx = idx++;
        return references.get(refIdx).getVolume();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public Iterator<FsVolumeSpi> iterator() {
      return new FsVolumeSpiIterator(references);
    }

    /**
     * 获取当前有效卷的数量
     * @return 有效卷数量
     */
    public int size() {
      return references.size();
    }

    /**
     * 根据索引获取卷对象
     * @param index 索引位置
     * @return 对应索引的卷对象
     */
    public FsVolumeSpi get(int index) {
      return references.get(index).getVolume();
    }

    /**
     * 根据索引获取卷引用对象
     * @param index 索引位置
     * @return 对应索引的卷引用对象
     */
    public FsVolumeReference getReference(int index) {
      return references.get(index);
    }

    @Override
    public void close() throws IOException {
      // 释放所有卷引用，记录最后一个异常
      IOException ioe = null;
      for (FsVolumeReference ref : references) {
        try {
          ref.close();
        } catch (IOException e) {
          ioe = e;
        }
      }
      references.clear();
      if (ioe != null) {
        throw ioe;
      }
    }
  }

  /**
   * 获取所有卷的引用列表，调用者必须调用close方法释放引用
   * @return 包含所有卷引用的FsVolumeReferences对象
   */
  FsVolumeReferences getFsVolumeReferences();

  /**
   * 向数据集添加新的存储卷，如果支持块扫描会自动注册到块扫描器
   * @param location 新卷的存储位置
   * @param nsInfos 新卷对应的命名空间信息
   * @throws IOException 添加失败时抛出IO异常
   */
  void addVolume(
      final StorageLocation location,
      final List<NamespaceInfo> nsInfos) throws IOException;

  /**
   * 从数据集移除指定集合的存储卷，如果支持块扫描会自动从块扫描器移除
   * @param volumes 待移除的卷位置集合
   * @param clearFailure 是否清除卷的失败标记
   */
  void removeVolumes(Collection<StorageLocation> volumes, boolean clearFailure);

  /**
   * 根据存储ID获取对应的DatanodeStorage对象
   * @param storageUuid 存储的唯一ID
   * @return 对应ID的存储对象
   */
  DatanodeStorage getStorage(final String storageUuid);

  /**
   * 获取所有已挂载卷的存储报告
   * @param bpid 块池ID
   * @return 存储报告数组
   * @throws IOException 获取失败时抛出IO异常
   */
  StorageReport[] getStorageReports(String bpid)
      throws IOException;

  /**
   * 获取指定块所在的卷
   * @param b 扩展块对象
   * @return 包含该块副本的卷
   */
  V getVolume(ExtendedBlock b);

  /**
   * 获取所有卷的信息映射表
   * @return 卷名到卷信息的映射表
   */
  Map<String, Object> getVolumeInfoMap();

  /**
   * 获取卷失败统计信息
   * @return 卷失败统计信息，可能为null
   */
  VolumeFailureSummary getVolumeFailureSummary();

  /**
   * 获取指定块池所有已完成块的副本引用列表
   * 调用该方法前需要先获取数据集读写锁，避免遍历时块状态发生变更
   * @param bpid 块池ID
   * @return 指定块池所有已完成块的副本信息列表
   */
  List<ReplicaInfo> getFinalizedBlocks(String bpid);

  /**
   * 检查内存中的块记录是否与磁盘上的实际块一致，不一致时更新记录或标记损坏
   * @param bpid 块池ID
   * @param info 卷扫描信息
   * @throws IOException 检查更新过程中IO异常
   */
  void checkAndUpdate(String bpid, ScanInfo info) throws IOException;

  /**
   * 获取指定块元数据的输入流
   * @param b 扩展块对象
   * @return 元数据输入流，如果元数据不存在返回null
   * @throws IOException 获取过程中IO异常
   */
  LengthInputStream getMetaDataInputStream(ExtendedBlock b
      ) throws IOException;

  /**
   * 获取指定块在磁盘上的数据长度（不含元数据）
   * @param b 扩展块对象
   * @return 块数据长度（字节）
   * @throws IOException 获取长度过程中IO异常
   */
  long getLength(ExtendedBlock b) throws IOException;

  /**
   * 从副本映射表获取指定块的副本对象，已废弃
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return 副本对象
   */
  @Deprecated
  Replica getReplica(String bpid, long blockId);

  /**
   * 获取指定块的副本信息字符串
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return 副本信息字符串
   */
  String getReplicaString(String bpid, long blockId);

  /**
   * 从磁盘读取块，获取存储的生成时间戳
   * @param bpid 块池ID
   * @param blkid 块ID
   * @return 包含生成时间戳的Block对象
   * @throws IOException 读取过程中IO异常
   */
  Block getStoredBlock(String bpid, long blkid) throws IOException;

  /**
   * 获取指定块从指定偏移开始的输入流
   * @param b 块对象
   * @param seekOffset 起始偏移量
   * @return 块数据输入流
   * @throws IOException 获取输入流过程中IO异常
   */
  InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
            throws IOException;

  /**
   * 获取未完成临时块的输入流，包含数据和校验码流
   * @param b 块对象
   * @param blkoff 数据起始偏移
   * @param ckoff 校验码起始偏移
   * @return 包含数据和校验码的输入流对象
   * @throws IOException 获取输入流过程中IO异常
   */
  ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException;

  /**
   * 创建一个临时状态的块副本，返回副本元信息
   * @param storageType 存储类型
   * @param storageId 存储ID
   * @param b 块对象
   * @param isTransfer 是否是传输过程中创建
   * @return 新创建的临时副本处理器
   * @throws IOException 创建过程中IO异常
   */
  ReplicaHandler createTemporary(StorageType storageType, String storageId,
      ExtendedBlock b, boolean isTransfer) throws IOException;

  /**
   * 创建一个RBW（正在被写入）状态的块副本，返回副本元信息
   * @param storageType 存储类型
   * @param storageId 存储ID
   * @param b 块对象
   * @param allowLazyPersist 是否允许延迟持久化到磁盘
   * @return 新创建的RBW副本处理器
   * @throws IOException 创建过程中IO异常
   */
  ReplicaHandler createRbw(StorageType storageType, String storageId,
      ExtendedBlock b, boolean allowLazyPersist) throws IOException;

  /**
   * 创建一个RBW（正在被写入）状态的块副本，指定生成时间戳，返回副本元信息
   * @param storageType 存储类型
   * @param storageId 存储ID
   * @param b 块对象
   * @param allowLazyPersist 是否允许延迟持久化到磁盘
   * @param newGS 新的生成时间戳
   * @return 新创建的RBW副本处理器
   * @throws IOException 创建过程中IO异常
   */
  ReplicaHandler createRbw(StorageType storageType, String storageId,
      ExtendedBlock b, boolean allowLazyPersist, long newGS) throws IOException;

  /**
   * 恢复一个RBW状态的块副本，返回恢复后的副本元信息
   * @param b 块对象
   * @param newGS 恢复后的新生成时间戳
   * @param minBytesRcvd 副本最小可接受字节数
   * @param maxBytesRcvd 副本最大可接受字节数
   * @return 恢复后的RBW副本处理器
   * @throws IOException 恢复过程中IO异常
   */
  ReplicaHandler recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException;

  /**
   * 将临时状态的副本转换为RBW状态
   * @param temporary 待转换的临时块
   * @return 转换后的RBW副本对象
   * @throws IOException 转换过程中IO异常
   */
  ReplicaInPipeline convertTemporaryToRbw(
      ExtendedBlock temporary) throws IOException;

  /**
   * 追加写入一个已完成的块副本，返回副本元信息
   * @param b 块对象
   * @param newGS 新的生成时间戳
   * @param expectedBlockLen 追加后期望的块长度
   * @return 追加后的副本处理器
   * @throws IOException 追加过程中IO异常
   */
  ReplicaHandler append(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException;

  /**
   * 恢复失败的追加操作，返回恢复后的副本元信息
   * @param b 块对象
   * @param newGS 新的生成时间戳
   * @param expectedBlockLen 恢复后期望的块长度
   * @return 恢复后的副本处理器
   * @throws IOException 恢复过程中IO异常
   */
  ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException;
  
  /**
   * 恢复失败的数据管道关闭操作，更新生成时间戳，如果是RBW块则完成收尾
   * @param b 块对象
   * @param newGS 新的生成时间戳
   * @param expectedBlockLen 恢复后期望的块长度
   * @return 存储该副本的存储UUID
   * @throws IOException 恢复过程中IO异常
   */
  Replica recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
      ) throws IOException;
  
  /**
   * 完成块写入，将RBW块标记为已完成
   * @param b 待完成的块对象
   * @param fsyncDir 是否将目录变更同步到持久化设备
   * @throws IOException 完成过程中IO异常
   * @throws ReplicaNotFoundException 找不到对应副本时抛出
   */
  void finalizeBlock(ExtendedBlock b, boolean fsyncDir) throws IOException;

  /**
   * 取消块写入，删除临时文件并回滚状态
   * @param b 待取消的块对象
   * @