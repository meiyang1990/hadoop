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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件存储副本抽象基类，用于表示存储在DataNode本地磁盘上的数据块副本，封装了本地文件操作逻辑。
 * 所有存在于本地存储介质、由文件系统文件支撑的副本均继承此类。
 */
abstract public class LocalReplica extends ReplicaInfo {

  /**
   * 基础目录，包含按块ID划分的子目录或直接存放块文件。
   */
  private File baseDir;

  /**
   * 标识副本父目录是否使用分块子目录结构，若为true则可根据块ID生成子目录路径。
   */
  private boolean hasSubdirs;

  /**
   * 基础目录字符串到File对象的缓存池，用于复用相同路径的File对象减少内存开销。
   */
  private static final Map<String, File> internedBaseDirs = new HashMap<String, File>();

  static final Logger LOG = LoggerFactory.getLogger(LocalReplica.class);

  /**
   * 构造函数，根据已有Block对象创建本地副本实例。
   * @param block 块对象
   * @param vol 副本所在存储卷
   * @param dir 块数据文件和元数据文件所在目录
   */
  LocalReplica(Block block, FsVolumeSpi vol, File dir) {
    this(block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp(), vol, dir);
  }

  /**
   * 构造函数，根据块基本信息创建本地副本实例。
   * @param blockId 块ID
   * @param len 副本长度
   * @param genStamp 副本版本号（生成戳）
   * @param vol 副本所在存储卷
   * @param dir 块数据文件和元数据文件所在目录
   */
  LocalReplica(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir) {
    super(vol, blockId, len, genStamp);
    setDirInternal(dir);
  }

  /**
   * 拷贝构造函数，从已有LocalReplica创建新实例。
   * @param from 源副本对象
   */
  LocalReplica(LocalReplica from) {
    this(from, from.getVolume(), from.getDir());
  }

  /**
   * 获取当前副本数据文件的完整路径。
   * @return 数据文件完整路径
   */
  @VisibleForTesting
  public File getBlockFile() {
    return new File(getDir(), getBlockName());
  }

  /**
   * 获取当前副本元数据文件的完整路径。
   * @return 元数据文件完整路径
   */
  @VisibleForTesting
  public File getMetaFile() {
    return new File(getDir(),
        DatanodeUtil.getMetaName(getBlockName(), getGenerationStamp()));
  }

  /**
   * 获取当前副本所在的父目录路径，根据是否使用分质子目录自动生成路径。
   * @return 副本所在父目录
   */
  protected File getDir() {
    return hasSubdirs ? DatanodeUtil.idToBlockDir(baseDir,
        getBlockId()) : baseDir;
  }

  /**
   * 设置副本所在父目录，解析目录结构并复用基础目录File对象。
   * @param dir 副本所在父目录
   */
  private void setDirInternal(File dir) {
    if (dir == null) {
      baseDir = null;
      return;
    }

    // 解析目录得到基础目录和是否包含分质子目录信息
    ReplicaDirInfo dirInfo = parseBaseDir(dir, getBlockId());
    this.hasSubdirs = dirInfo.hasSubidrs;

    // 同步缓存池，复用已有的基础目录File对象
    synchronized (internedBaseDirs) {
      if (!internedBaseDirs.containsKey(dirInfo.baseDirPath)) {
        // 创建新File对象并缓存，释放原始char[]引用节省内存
        File baseDir = new File(dirInfo.baseDirPath);
        internedBaseDirs.put(dirInfo.baseDirPath, baseDir);
      }
      this.baseDir = internedBaseDirs.get(dirInfo.baseDirPath);
    }
  }

  /**
   * 存储目录解析结果信息类，保存基础目录路径和是否包含分质子目录。
   */
  @VisibleForTesting
  public static class ReplicaDirInfo {
    public String baseDirPath;
    public boolean hasSubidrs;

    public ReplicaDirInfo (String baseDirPath, boolean hasSubidrs) {
      this.baseDirPath = baseDirPath;
      this.hasSubidrs = hasSubidrs;
    }
  }

  /**
   * 解析副本目录，识别是否使用分质子目录结构，提取基础目录路径。
   * @param dir 副本所在完整目录
   * @param blockId 块ID
   * @return 解析结果包含基础目录路径和是否包含分质子目录标志
   */
  @VisibleForTesting
  public static ReplicaDirInfo parseBaseDir(File dir, long blockId) {
    File currentDir = dir;
    boolean hasSubdirs = false;
    // 向上遍历找到第一个不是块子目录前缀的目录，即为基础目录
    while (currentDir.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX)) {
      hasSubdirs = true;
      currentDir = currentDir.getParentFile();
    }
    // 如果遍历到分质子目录，验证目录是否符合分块路径规则
    if (hasSubdirs) {
      File idToBlockDir = DatanodeUtil.idToBlockDir(currentDir, blockId);
      if (idToBlockDir.equals(dir)) {
        return new ReplicaDirInfo(currentDir.getAbsolutePath(), true);
      }
    }
    // 无分质子目录，直接返回原目录
    return new ReplicaDirInfo(dir.getAbsolutePath(), false);
  }

  /**
   * 通过复制文件打断硬链接，将原文件替换为新的独立文件。
   * 用于升级后需要修改文件时，保证旧硬链接指向的快照不受影响。
   * @param file 需要打断硬链接的文件
   * @param b 所属块对象
   * @throws IOException 操作失败抛出IO异常
   */
  private void breakHardlinks(File file, Block b) throws IOException {
    final FileIoProvider fileIoProvider = getFileIoProvider();
    // 在同目录创建临时文件
    final File tmpFile = DatanodeUtil.createFileWithExistsCheck(
        getVolume(), b, DatanodeUtil.getUnlinkTmpFile(file), fileIoProvider);
    try {
      // 从原文件读取数据写入临时文件
      try (FileInputStream in = fileIoProvider.getFileInputStream(
          getVolume(), file)) {
        try (FileOutputStream out = fileIoProvider.getFileOutputStream(
            getVolume(), tmpFile)) {
          IOUtils.copyBytes(in, out, 16 * 1024);
        }
      }
      // 校验复制后文件长度
      if (file.length() != tmpFile.length()) {
        throw new IOException("Copy of file " + file + " size " + file.length()
            + " into file " + tmpFile + " resulted in a size of "
            + tmpFile.length());
      }
      // 将临时文件替换原文件，完成硬链接打断
      fileIoProvider.replaceFile(getVolume(), tmpFile, file);
    } catch (IOException e) {
      // 失败删除临时文件
      if (!fileIoProvider.delete(getVolume(), tmpFile)) {
        DataNode.LOG.info("detachFile failed to delete temporary file " +
                          tmpFile);
      }
      throw e;
    }
  }

  /**
   * 检查并打断当前副本数据文件和元数据文件的硬链接，用于DataNode升级后修改文件。
   * 升级过程中创建硬链接共享数据，当需要修改文件时必须打断硬链接，保证旧版本目录回滚时数据一致性。
   * @return 始终返回true表示已完成复制操作
   * @throws IOException 操作失败抛出IO异常
   */
  public boolean breakHardLinksIfNeeded() throws IOException {
    final File file = getBlockFile();
    final FileIoProvider fileIoProvider = getFileIoProvider();
    if (file == null || getVolume() == null) {
      throw new IOException("detachBlock:Block not found. " + this);
    }
    File meta = getMetaFile();

    // 获取数据文件硬链接计数，如果大于1则打断
    int linkCount = fileIoProvider.getHardLinkCount(getVolume(), file);
    if (linkCount > 1) {
      DataNode.LOG.info("Breaking hardlink for " + linkCount + "x-linked " +
          "block " + this);
      breakHardlinks(file, this);
    }
    // 同样处理元数据文件
    if (fileIoProvider.getHardLinkCount(getVolume(), meta) > 1) {
      breakHardlinks(meta, this);
    }
    return true;
  }

  @Override
  public URI getBlockURI() {
    return getBlockFile().toURI();
  }

  @Override
  public InputStream getDataInputStream(long seekOffset) throws IOException {
    return getDataInputStream(getBlockFile(), seekOffset);
  }

  @Override
  public OutputStream getDataOutputStream(boolean append) throws IOException {
    return getFileIoProvider().getFileOutputStream(
        getVolume(), getBlockFile(), append);
  }

  @Override
  public boolean blockDataExists() {
    return getFileIoProvider().exists(getVolume(), getBlockFile());
  }

  @Override
  public boolean deleteBlockData() {
    return getFileIoProvider().fullyDelete(getVolume(), getBlockFile());
  }

  @Override
  public long getBlockDataLength() {
    return getBlockFile().length();
  }

  @Override
  public URI getMetadataURI() {
    return getMetaFile().toURI();
  }

  @Override
  public LengthInputStream getMetadataInputStream(long offset)
      throws IOException {
    final File meta = getMetaFile();
    // NativeIO可用时使用支持删除后仍可读的输入流
    if (NativeIO.isAvailable()) {
      return new LengthInputStream(
          getFileIoProvider().getShareDeleteFileInputStream(
              getVolume(), meta, offset),
          meta.length());
    }
    // NativeIO不可用，使用普通打开并定位的方式
    return new LengthInputStream(
        getFileIoProvider.openAndSeek(getVolume(), meta, offset),
        meta.length());
  }

  @Override
  public OutputStream getMetadataOutputStream(boolean append)
      throws IOException {
    return new FileOutputStream(getMetaFile(), append);
  }

  @Override
  public boolean metadataExists() {
    return getFileIoProvider().exists(getVolume(), getMetaFile());
  }

  @Override
  public boolean deleteMetadata() {
    return getFileIoProvider().fullyDelete(getVolume(), getMetaFile());
  }

  @Override
  public long getMetadataLength() {
    return getMetaFile().length();
  }

  @Override
  public boolean renameMeta(URI destURI) throws IOException {
    return renameFile(getMetaFile(), new File(destURI));
  }

  @Override
  public boolean renameData(URI destURI) throws IOException {
    return renameFile(getBlockFile(), new File(destURI));
  }

  /**
   * 重命名文件，封装错误处理。
   * @param srcfile 源文件
   * @param destfile 目标文件
   * @return 重命名成功返回true
   * @throws IOException 重命名失败抛出IO异常
   */
  private boolean renameFile(File srcfile, File destfile) throws IOException {
    try {
      getFileIoProvider().rename(getVolume(), srcfile, destfile);
      return true;
    } catch (IOException e) {
      throw new IOException("Failed to move block file for " + this
          + " from " + srcfile + " to " + destfile.getAbsolutePath(), e);
    }
  }

  @Override
  public void updateWithReplica(StorageLocation replicaLocation) {
    // 本地副本假设位置是文件路径，解析URI获取父目录
    File diskFile = null;
    try {
      diskFile = new File(replicaLocation.getUri());
    } catch (IllegalArgumentException e) {
      diskFile = null;
    }

    // 更新目录信息
    if (null == diskFile) {
      setDirInternal(null);
    } else {
      setDirInternal(diskFile.getParentFile());
    }
  }

  @Override
  public boolean getPinning(LocalFileSystem localFS) throws IOException {
    return getPinning(localFS, new Path(getBlockFile().getAbsolutePath()));
  }

  @Override
  public void setPinning(LocalFileSystem localFS) throws IOException {
    File f = getBlockFile();
    Path p = new Path(f.getAbsolutePath());
    setPinning(localFS, p);
  }

  /**
   * 更新副本版本号，重命名元数据文件匹配新版本号。
   * @param newGS 新版本号
   * @throws IOException 修改失败抛出IO异常
   */
  @Override
  public void bumpReplicaGS(long newGS) throws IOException {
    long oldGS = getGenerationStamp();
    final File oldmeta = getMetaFile();
    // 更新内存版本号，获取新元数据路径
    setGenerationStamp(newGS);
    final File newmeta = getMetaFile();

    // 重命名元数据文件到新路径
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    try {
      getFileIoProvider().rename(getVolume(), oldmeta, newmeta);
    } catch (IOException e) {
      // 重命名失败恢复旧版本号
      setGenerationStamp(oldGS);
      throw new IOException("Block " + this + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to " + newmeta, e);
    }
  }

  @Override
  public void truncateBlock(long newLength) throws IOException {
    truncateBlock(getVolume(), getBlockFile(), getMetaFile(),
        getNumBytes(), newLength, getFileIoProvider());
  }

  @Override
  public int compareWith(ScanInfo info) {
    return info.getBlockFile().compareTo(getBlockFile());
  }

  @Override
  public void copyMetadata(URI destination) throws IOException {
    // 本地副本假设目标是文件，直接拷贝
    getFileIoProvider().nativeCopyFileUnbuffered(
        getVolume(), getMetaFile(), new File(destination), true);
  }

  @Override
  public void copyBlockdata(URI destination) throws IOException {
    // 本地副本假设目标是文件，直接拷贝
    getFileIoProvider().nativeCopyFileUnbuffered(
        getVolume(), getBlockFile(), new File(destination), true);
  }

  /**
   * 获取本地数据文件输入流，根据是否支持NativeIO选择不同打开方式，并定位到指定偏移。
   * @param f 数据文件路径
   * @param seekOffset 需要定位的偏移量
   * @return 定位后的文件输入流
   * @throws IOException 打开或定位失败抛出IO异常
   */
  private FileInputStream getDataInputStream(File f, long seekOffset)
      throws IOException {
    FileInputStream fis;
    final FileIoProvider fileIoProvider = getFileIoProvider();
    if (NativeIO.isAvailable()) {
      // 使用支持共享删除的输入流
      fis = fileIoProvider.getShareDeleteFileInputStream(
          getVolume(), f, seekOffset);
    } else {
      try {
        // 普通打开并定位
        fis = fileIoProvider.openAndSeek(getVolume(), f, seekOffset);
      } catch