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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.util.LightWeightResizableGSet;

/**
 * 文件概要：HDFS DataNode节点存储副本元数据的抽象基类，定义了副本元数据的通用接口，
 * 用于维护DataNode上所有数据块副本的元信息，支持不同类型副本的统一管理。
 * <p>
 * 本类是DataNode维护副本元数据的核心抽象，提供了副本基本属性、文件IO操作、恢复处理的统一接口，
 * 具体实现由不同状态/类型的副本子类完成。
 */
@InterfaceAudience.Private
abstract public class ReplicaInfo extends Block
    implements Replica, LightWeightResizableGSet.LinkedElement {

  /** 用于LightWeightResizableGSet链表实现，维护链表下一个节点引用 */
  private LightWeightResizableGSet.LinkedElement next;

  /** 此副本所在的存储卷 */
  private FsVolumeSpi volume;

  /** 默认文件IO提供者，用于测试场景和FsDatasetUtil计算校验和，当volume为null时使用 */
  private static final FileIoProvider DEFAULT_FILE_IO_PROVIDER =
      new FileIoProvider(null, null);

  /**
   * 构造方法，通过已有Block对象和存储卷创建副本信息
   * @param block 块对象，包含块ID、长度、生成时间戳
   * @param vol 副本所在存储卷
   */
  ReplicaInfo(Block block, FsVolumeSpi vol) {
    this(vol, block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp());
  }

  /**
   * 构造方法，通过指定参数创建副本信息
   * @param vol 副本所在存储卷
   * @param blockId 块ID
   * @param len 副本长度
   * @param genStamp 副本生成时间戳
   */
  ReplicaInfo(FsVolumeSpi vol, long blockId, long len, long genStamp) {
    super(blockId, len, genStamp);
    this.volume = vol;
  }
  
  /**
   * 拷贝构造方法，从已有副本信息创建新副本
   * @param from 要拷贝的源副本信息
   */
  ReplicaInfo(ReplicaInfo from) {
    this(from, from.getVolume());
  }

  /**
   * 获取此副本所在的存储卷
   * @return 副本所在存储卷对象
   */
  public FsVolumeSpi getVolume() {
    return volume;
  }

  /**
   * 获取用于磁盘IO操作的文件IO提供者
   * @return 文件IO提供者对象
   */
  public FileIoProvider getFileIoProvider() {
    // 当volume未知（测试场景或computeChecksum调用）时，使用默认无钩子的IO提供者
    return (volume != null) ? volume.getFileIoProvider()
        : DEFAULT_FILE_IO_PROVIDER;
  }

  /**
   * 设置此副本所在的存储卷
   * @param vol 要设置的存储卷对象
   */
  void setVolume(FsVolumeSpi vol) {
    this.volume = vol;
  }

  /**
   * 获取存储此副本的存储卷的UUID
   * @return 存储卷UUID字符串
   */
  @Override
  public String getStorageUuid() {
    return volume.getStorageID();
  }

  /**
   * 获取此副本在磁盘上预留的字节数
   * @return 预留字节数，默认实现返回0
   */
  public long getBytesReserved() {
    return 0;
  }

  /**
   * 获取此副本数据文件存储位置的URI
   * @return 副本数据文件位置URI
   */
  abstract public URI getBlockURI();

  /**
   * 打开并获取副本数据文件的输入流，从指定偏移开始读取
   * @param seekOffset 读取起始偏移量
   * @return 用于读取副本数据的输入流
   * @throws IOException 打开流失败时抛出IO异常
   */
  abstract public InputStream getDataInputStream(long seekOffset)
      throws IOException;

  /**
   * 打开并获取副本数据文件的输出流
   * @param append 是否为追加模式打开
   * @return 用于写入副本数据的输出流
   * @throws IOException 创建输出流失败时抛出IO异常
   */
  abstract public OutputStream getDataOutputStream(boolean append)
      throws IOException;

  /**
   * 检查副本数据文件是否存在
   * @return 数据文件存在返回true，否则返回false
   */
  abstract public boolean blockDataExists();

  /**
   * 删除副本数据文件
   * @return 删除成功返回true，否则返回false
   */
  abstract public boolean deleteBlockData();

  /**
   * 获取存储设备上副本数据文件的实际长度
   * @return 数据文件长度（字节）
   */
  abstract public long getBlockDataLength();

  /**
   * 获取此副本元数据文件存储位置的URI
   * @return 副本元数据文件位置URI
   */
  abstract public URI getMetadataURI();

  /**
   * 打开并获取副本元数据文件的输入流，从指定偏移开始读取
   * @param offset 读取起始偏移量
   * @return 用于读取元数据的长度输入流
   * @throws IOException 打开流失败时抛出IO异常
   */
  abstract public LengthInputStream getMetadataInputStream(long offset)
      throws IOException;

  /**
   * 打开并获取副本元数据文件的输出流
   * @param append 是否为追加模式打开
   * @return 用于写入元数据的输出流
   * @throws IOException 创建输出流失败时抛出IO异常
   */
  abstract public OutputStream getMetadataOutputStream(boolean append)
      throws IOException;

  /**
   * 检查副本元数据文件是否存在
   * @return 元数据文件存在返回true，否则返回false
   */
  abstract public boolean metadataExists();

  /**
   * 删除副本元数据文件
   * @return 删除成功返回true，否则返回false
   */
  abstract public boolean deleteMetadata();

  /**
   * 获取存储设备上副本元数据文件的实际长度
   * @return 元数据文件长度（字节）
   */
  abstract public long getMetadataLength();

  /**
   * 将元数据文件重命名到目标URI
   * @param destURI 目标URI
   * @return 重命名成功返回true，否则返回false
   * @throws IOException 重命名过程发生IO异常时抛出
   */
  abstract public boolean renameMeta(URI destURI) throws IOException;

  /**
   * 将数据文件重命名到目标URI
   * @param destURI 目标URI
   * @return 重命名成功返回true，否则返回false
   * @throws IOException 重命名过程发生IO异常时抛出
   */
  abstract public boolean renameData(URI destURI) throws IOException;

  /**
   * 使用扫描到的存储位置更新当前副本信息
   * @param replicaLocation 扫描发现的副本存储位置
   */
  abstract public void updateWithReplica(StorageLocation replicaLocation);

  /**
   * 检查此块是否被固定（固定后不会被Balancer/Mover迁移）
   * @param localFS 本地文件系统
   * @return 块被固定返回true，否则返回false
   * @throws IOException 读取固定标记发生IO异常时抛出
   */
  abstract public boolean getPinning(LocalFileSystem localFS)
      throws IOException;

  /**
   * 设置此块为固定状态，固定后不会被Balancer/Mover迁移
   * @param localFS 本地文件系统
   * @throws IOException 设置固定标记发生IO异常时抛出
   */
  abstract public void setPinning(LocalFileSystem localFS) throws IOException;

  /**
   * 将副本生成时间戳更新为新值，同时重命名磁盘上的元数据文件
   * @param newGS 新的生成时间戳
   * @throws IOException 更新失败时抛出IO异常
   */
  abstract public void bumpReplicaGS(long newGS) throws IOException;

  /**
   * 获取此副本的原始副本对象（用于增量副本等场景）
   * @return 原始副本对象
   */
  abstract public ReplicaInfo getOriginalReplica();

  /**
   * 获取恢复ID，即恢复后副本将更新到的生成时间戳
   * @return 恢复ID（新生成时间戳）
   */
  abstract public long getRecoveryID();

  /**
   * 设置恢复ID
   * @param recoveryId 新的恢复ID
   */
  abstract public void setRecoveryID(long recoveryId);

  /**
   * 在需要时断开硬链接（用于副本恢复等场景）
   * @return 操作成功返回true
   * @throws IOException 操作发生IO异常时抛出
   */
  abstract public boolean breakHardLinksIfNeeded() throws IOException;

  /**
   * 创建副本恢复信息对象，用于块恢复流程
   * @return 副本恢复信息
   */
  abstract public ReplicaRecoveryInfo createInfo();

  /**
   * 将当前副本信息与卷扫描得到的信息进行比对
   * @param info 卷扫描得到的扫描信息
   * @return 比对结果状态码
   */
  abstract public int compareWith(ScanInfo info);

  /**
   * 将副本块截断到指定长度
   * @param newLength 目标截断长度
   * @throws IOException 截断操作失败时抛出IO异常
   */
  abstract public void truncateBlock(long newLength) throws IOException;

  /**
   * 将元数据复制到目标URI位置
   * @param destination 目标URI
   * @throws IOException 复制过程发生IO异常时抛出
   */
  abstract public void copyMetadata(URI destination) throws IOException;

  /**
   * 将数据复制到目标URI位置
   * @param destination 目标URI
   * @throws IOException 复制过程发生IO异常时抛出
   */
  abstract public void copyBlockdata(URI destination) throws IOException;

  /**
   * 获取此副本最初预留的字节数，实际预留会随写入调整
   * @return 最初预留字节数，默认实现返回0
   */
  public long getOriginalBytesReserved() {
    return 0;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + ", " + super.toString()
        + ", " + getState()
        + "\n  getNumBytes()     = " + getNumBytes()
        + "\n  getBytesOnDisk()  = " + getBytesOnDisk()
        + "\n  getVisibleLength()= " + getVisibleLength()
        + "\n  getVolume()       = " + getVolume()
        + "\n  getBlockURI()     = " + getBlockURI();
  }

  @Override
  public boolean isOnTransientStorage() {
    return volume.isTransientStorage();
  }

  @Override
  public LightWeightResizableGSet.LinkedElement getNext() {
    return next;
  }

  @Override
  public void setNext(LightWeightResizableGSet.LinkedElement next) {
    this.next = next;
  }
}