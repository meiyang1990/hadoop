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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;

/**
 * HDFS DataNode 存储卷的服务提供者接口，定义了存储卷的核心能力抽象。
 * 不同类型的存储实现（如磁盘、内存、云存储等）都需要实现该接口，为上层提供统一存储能力。
 */
public interface FsVolumeSpi
    extends Checkable<FsVolumeSpi.VolumeCheckContext, VolumeCheckResult> {

  /**
   * 获取存储卷引用，增加存储卷的引用计数，防止存储卷被并发释放。
   * 调用方负责在使用完成后关闭引用对象，以减少引用计数。
   * @return 存储卷引用对象
   * @throws ClosedChannelException 如果存储卷已关闭抛出异常
   */
  FsVolumeReference obtainReference() throws ClosedChannelException;

  /**
   * 获取存储卷的唯一存储ID。
   * @return 存储卷的UUID字符串
   */
  String getStorageID();

  /**
   * 获取该存储卷承载的所有块池ID列表。
   * @return 块池ID数组
   */
  String[] getBlockPoolList();

  /**
   * 获取存储卷当前可用存储空间大小，单位为字节。
   * @return 可用字节数
   * @throws IOException IO异常
   */
  long getAvailable() throws IOException;

  /**
   * 获取存储卷的根路径URI。
   * @return 存储卷根URI
   */
  URI getBaseURI();

  /**
   * 获取存储卷的磁盘使用统计信息。
   * @param conf Hadoop配置对象
   * @return 磁盘使用统计对象
   */
  DF getUsageStats(Configuration conf);

  /**
   * 获取存储卷的存储位置对象。
   * @return 存储位置对象
   */
  StorageLocation getStorageLocation();

  /**
   * 获取存储卷的存储类型。
   * @return 存储类型枚举（DISK/SSD/ARCHIVE等）
   */
  StorageType getStorageType();

  /**
   * 判断存储卷是否为非持久化存储（非持久化存储重启后数据丢失）。
   * @return true表示是非持久化存储，false表示是持久化存储
   */
  boolean isTransientStorage();

  /**
   * 判断存储卷是否基于RAM内存存储。
   * @return true表示是内存存储，false表示不是
   */
  boolean isRAMStorage();

  /**
   * 为正在写入的块（RBW块或重复制块）预留磁盘空间，避免写入过程中存储空间耗尽。
   * @param bytesToReserve 需要预留的字节数
   */
  void reserveSpaceForReplica(long bytesToReserve);

  /**
   * 释放之前为写入块预留的磁盘空间。
   * @param bytesToRelease 需要释放的字节数
   */
  void releaseReservedSpace(long bytesToRelease);

  /**
   * 释放瞬态存储（RAM）中RBW块占用的锁定内存。
   * 释放大小会向下舍入到操作系统页大小的倍数，因为锁定内存必须是页大小的整数倍。
   * @param bytesToRelease 需要释放的字节数
   */
  void releaseLockedMemory(long bytesToRelease);

  /**
   * 块迭代器接口，用于按排序顺序遍历存储卷中某个块池的所有块。
   * 迭代器不同步，同一时间只能由单个线程使用，需要调用save方法才能将迭代器状态持久化到磁盘。
   */
  interface BlockIterator extends Closeable {
    /**
     * 获取下一个块信息。
     * 注意返回的块可能已经被删除或为过期条目，调用方需要处理块不存在的场景。
     * @return 下一个扩展块，没有更多块或发生错误时返回null
     * @throws IOException 获取下一个块时发生IO异常
     */
    ExtendedBlock nextBlock() throws IOException;

    /**
     * 判断是否已经遍历到块池末尾。
     * @return true表示已经到末尾，false表示还有更多块
     */
    boolean atEnd();

    /**
     * 将迭代器重置回块池开头，重新开始遍历。
     */
    void rewind();

    /**
     * 将迭代器当前状态保存到存储卷磁盘，同名已有迭代器会被覆盖。
     * @throws IOException 保存过程中发生IO异常
     */
    void save() throws IOException;

    /**
     * 设置允许返回条目的最大过期时间。
     * 0表示不允许返回过期条目；更大的值允许通过接受一定过期风险降低资源消耗。
     * 即使设置为0，调用方仍然需要处理块在处理前被删除的竞态条件。
     * @param maxStalenessMs 最大过期时间，单位毫秒
     */
    void setMaxStalenessMs(long maxStalenessMs);

    /**
     * 获取迭代器创建时间，单位为从纪元开始的毫秒数。
     * @return 迭代器创建时间戳
     */
    long getIterStartMs();

    /**
     * 获取迭代器上次保存时间，单位为从纪元开始的毫秒数。从未保存则返回创建时间。
     * @return 上次保存时间戳
     */
    long getLastSavedMs();

    /**
     * 获取当前迭代器遍历的块池ID。
     * @return 块池ID
     */
    String getBlockPoolId();
  }

  /**
   * 创建一个新的块迭代器，从块集开头开始遍历。
   * @param bpid 需要遍历的块池ID
   * @param name 新块迭代器名称
   * @return 新建的块迭代器
   */
  BlockIterator newBlockIterator(String bpid, String name);

  /**
   * 从磁盘加载已保存的块迭代器。
   * @param bpid 需要遍历的块池ID
   * @param name 需要加载的块迭代器名称
   * @return 加载后的块迭代器
   * @throws IOException 加载过程中发生IO异常
   */
  BlockIterator loadBlockIterator(String bpid, String name) throws IOException;

  /**
   * 获取该存储卷所属的文件数据集对象。
   * @return 所属FsDatasetSpi对象
   */
  FsDatasetSpi getDataset();

  /**
   * 存储块扫描信息类，保存磁盘上块文件和元数据文件的路径信息。
   * 为节省内存，仅存储路径后缀，不存储完整路径。完整路径通过基路径拼接得到。
   */
  public static class ScanInfo implements Comparable<ScanInfo> {
    private final long blockId;
    /**
     * 存储块/元数据文件所在目录的完整路径
     */
    private final File basePath;
    /**
     * 块文件名，不包含路径
     */
    private final String blockFile;
    /**
     * 元数据文件名存储规则：
     * 如果blockFile为null，存储完整元数据文件名（不包含路径）
     * 如果blockFile不为null，仅存储元文件名后缀（元文件名=blockFile+后缀），以此节省内存
     */
    private final String metaFile;

    private final FsVolumeSpi volume;

    private final FileRegion fileRegion;
    /**
     * 异步块扫描中缓存的块文件长度
     */
    private final long blockLength;

    private final static Pattern CONDENSED_PATH_REGEX =
        Pattern.compile("(?<!^)(\\\\|/){2,}");

    private final static String QUOTED_FILE_SEPARATOR =
        Matcher.quoteReplacement(File.separator);

    /**
     * 获取路径相对于前缀的后缀部分。
     * @param f 完整路径字符串
     * @param prefix 需要去除的前缀
     * @return 路径后缀，满足前缀+后缀=完整路径
     */
    private static String getSuffix(String f, String prefix) {
      if (f.startsWith(prefix)) {
        return f.substring(prefix.length());
      }
      throw new RuntimeException(prefix + " is not a prefix of " + f);
    }

    /**
     * 构造普通存储块的ScanInfo对象，解析块文件和元数据文件信息。
     * @param blockId 块ID
     * @param basePath 块存储目录的完整路径
     * @param blockFile 块文件名，不包含路径
     * @param metaFile 元数据文件名，不包含路径；如果blockFile不为null，元文件名是块文件名加后缀
     * @param vol 块所属存储卷
     */
    public ScanInfo(long blockId, File basePath, String blockFile,
        String metaFile, FsVolumeSpi vol) {
      this.blockId = blockId;
      this.basePath = basePath;
      this.blockFile = blockFile;
      if (blockFile != null && metaFile != null) {
        this.metaFile = getSuffix(metaFile, blockFile);
      } else {
        this.metaFile = metaFile;
      }
      this.blockLength = (blockFile != null) ?
          new File(basePath, blockFile).length() : 0;
      this.volume = vol;
      this.fileRegion = null;
    }

    /**
     * 构造提供块的ScanInfo对象，用于传入已经准备好的块数据。
     * @param blockId 块ID
     * @param vol 块所属存储卷
     * @param fileRegion 块文件区域
     * @param length 块数据长度
     */
    public ScanInfo(long blockId, FsVolumeSpi vol, FileRegion fileRegion,
        long length) {
      this.blockId = blockId;
      this.blockLength = length;
      this.volume = vol;
      this.fileRegion = fileRegion;
      this.basePath = null;
      this.blockFile = null;
      this.metaFile = null;
    }

    /**
     * 获取块数据文件对象。
     * @return 块数据文件，不存在则返回null
     */
    public File getBlockFile() {
      return (blockFile == null) ? null :
          new File(basePath.getAbsolutePath(), blockFile);
    }

    /**
     * 获取块长度，返回创建对象时缓存的长度值。
     * @return 块数据长度，单位字节
     */
    public long getBlockLength() {
      return blockLength;
    }

    /**
     * 获取块元数据文件对象。
     * @return 元数据文件，不存在则返回null
     */
    public File getMetaFile() {
      if (metaFile == null) {
        return null;
      }
      return new File(basePath.getAbsolutePath(), fullMetaFile());
    }

    /**
     * 获取块ID。
     * @return 块ID
     */
    public long getBlockId() {
      return blockId;
    }

    /**
     * 获取该块所属的存储卷。
     * @return 存储卷对象
     */
    public FsVolumeSpi getVolume() {
      return volume;
    }

    @Override
    public int compareTo(ScanInfo b) {
      return Long.compare(this.blockId, b.blockId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ScanInfo)) {
        return false;
      }
      return blockId == ((ScanInfo) o).blockId;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(this.blockId);
    }

    /**
     * 获取块的生成时间戳。
     * @return 生成时间戳，没有元数据则返回默认祖代时间戳
     */
    public long getGenStamp() {
      return metaFile != null ? Block.getGenerationStamp(fullMetaFile())
          : HdfsConstants.GRANDFATHER_GENERATION_STAMP;
    }

    /**
     * 获取块文件区域对象。
     * @return 文件区域对象
     */
    public FileRegion getFileRegion() {
      return fileRegion;
    }

    /**
     * 拼接得到完整元文件名。
     * @return 完整元文件名，null表示元文件不存在
     */
    private String fullMetaFile() {
      if (metaFile == null) {
        return null;
      }
      if (blockFile == null) {
        return metaFile;
      } else {
        return blockFile + metaFile;
      }
    }
  }

  /**
   * 从校验文件加载最后一个部分块的校验和。
   * 需要在持有FsDataset锁的情况下调用。
   * @param blockFile 块数据文件
   * @param metaFile 元数据校验文件
   * @return 最后一个部分块的校验和字节数组
   * @throws IOException IO异常
   */
  byte[] loadLastPartialChunkChecksum(File blockFile, File metaFile)
      throws IOException;

  /**
   * 扫描指定块池，收集所有块的扫描信息编译成报告。
   * @param bpid 目标块池ID
   * @param report 收集扫描结果的集合
   * @param reportCompiler 报告编译器
   * @throws InterruptedException 线程中断异常
   * @throws IOException IO异常
   */
  void compileReport(String bpid,
      Collection<ScanInfo> report, ReportCompiler reportCompiler)
      throws InterruptedException, IOException;

  /**
   * 存储卷健康检查上下文，承载检查所需参数。
   */
  class VolumeCheckContext {
  }

  /**
   * 获取存储卷的文件IO提供者，用于统计和拦截IO操作。
   * @return 文件IO提供者对象
   */
  FileIoProvider getFileIoProvider();

  /**
   * 获取存储卷的指标收集器。
   * @return 存储卷指标对象
   */
  DataNodeVolumeMetrics getMetrics();
}