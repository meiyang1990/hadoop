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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

/**
 * 为数据节点提供通用工具方法的工具类，包含块文件路径生成、文件操作、磁盘错误处理等能力
 */
@InterfaceAudience.Private
public class DatanodeUtil {
  /** 已解除链接块文件的后缀名 */
  public static final String UNLINK_BLOCK_SUFFIX = ".unlinked";

  /** 磁盘错误异常前缀标识 */
  public static final String DISK_ERROR = "Possible disk error: ";

  private static final String SEP = System.getProperty("file.separator");
  /** 用于子目录掩码计算，限制子目录数量为32个 */
  private static final long MASK = 0x1F;

  /**
   * 从IO异常中提取磁盘错误的根原因，如果异常是磁盘错误则返回根异常，否则返回null
   * @param ioe 待检查的IO异常
   * @return 磁盘错误的根异常，若不是磁盘错误则返回null
   */ 
  static IOException getCauseIfDiskError(IOException ioe) {
    if (ioe.getMessage()!=null && ioe.getMessage().startsWith(DISK_ERROR)) {
      return (IOException)ioe.getCause();
    } else {
      return null;
    }
  }

  /**
   * 先检查文件是否存在，若不存在则创建新的临时块文件
   * @param volume 文件所属存储卷
   * @param b 当前操作的块对象
   * @param f 待创建的文件对象
   * @param fileIoProvider 文件操作提供者，封装不同存储实现的文件操作
   * @return 创建成功的文件对象
   * @throws IOException 文件已存在或创建失败时抛出异常
   */
  public static File createFileWithExistsCheck(
      FsVolumeSpi volume, Block b, File f,
      FileIoProvider fileIoProvider) throws IOException {
    if (fileIoProvider.exists(volume, f)) {
      throw new IOException("Failed to create temporary file for " + b
          + ".  File " + f + " should not be present, but is.");
    }
    // Create the zero-length temp file
    final boolean fileCreated;
    try {
      fileCreated = fileIoProvider.createFile(volume, f);
    } catch (IOException ioe) {
      throw new IOException(DISK_ERROR + "Failed to create " + f, ioe);
    }
    if (!fileCreated) {
      throw new IOException("Failed to create temporary file for " + b
          + ".  File " + f + " should be creatable, but is already present.");
    }
    return f;
  }
  
  /**
   * 根据块名称和生成时间戳生成块元数据文件名
   * @param blockName 块名称
   * @param generationStamp 块生成时间戳
   * @return 完整的块元数据文件名
   */
  public static String getMetaName(String blockName, long generationStamp) {
    return blockName + "_" + generationStamp + Block.METADATA_EXTENSION; 
  }

  /**
   * 根据原始块文件生成对应的解除链接临时文件对象
   * @param f 原始块文件
   * @return 解除链接临时文件对象
   */
  public static File getUnlinkTmpFile(File f) {
    return new File(f.getParentFile(), f.getName()+UNLINK_BLOCK_SUFFIX);
  }

  /**
   * 递归检查目录树中是否不存在任何文件（仅检查文件，目录本身不计入）
   * @param volume 目录所属存储卷
   * @param dir 待检查的根目录，必须已存在
   * @param fileIoProvider 文件操作提供者
   * @return 目录树中不存在任何文件返回true，否则返回false
   * @throws IOException 无法列出目录内容时抛出异常
   */
  public static boolean dirNoFilesRecursive(
      FsVolumeSpi volume, File dir,
      FileIoProvider fileIoProvider) throws IOException {
    File[] contents = fileIoProvider.listFiles(volume, dir);
    if (contents == null) {
      throw new IOException("Cannot list contents of " + dir);
    }
    for (File f : contents) {
      if (!f.isDirectory() ||
          (f.isDirectory() && !dirNoFilesRecursive(
              volume, f, fileIoProvider))) {
        return false;
      }
    }
    return true;
  }

  /**
   * 根据块ID计算块存储的两级子目录后缀路径，HDFS通过哈希分散块文件到不同子目录避免单目录文件过多
   * @param blockId 数据块ID
   * @return 两级子目录路径字符串，格式为subdirX/subdirY
   */
  public static String idToBlockDirSuffix(long blockId) {
    int d1 = (int) ((blockId >> 16) & MASK);
    int d2 = (int) ((blockId >> 8) & MASK);
    return DataStorage.BLOCK_SUBDIR_PREFIX + d1 + SEP +
        DataStorage.BLOCK_SUBDIR_PREFIX + d2;
  }

  /**
   * 根据根目录和块ID计算最终化块的存储目录，不自动创建目录
   * @param root 最终化块存储根目录
   * @param blockId 数据块ID
   * @return 块对应的存储目录对象
   */
  public static File idToBlockDir(File root, long blockId) {
    String path = idToBlockDirSuffix(blockId);
    return new File(root, path);
  }

  /**
   * 生成文件数据集锁需要扫描的所有两级子目录路径，用于遍历所有块存储目录
   * @return 所有可能的两级子目录路径列表
   */
  public static List<String> getAllSubDirNameForDataSetLock() {
    List<String> res = new ArrayList<>();
    for (int d1 = 0; d1 <= MASK; d1++) {
      for (int d2 = 0; d2 <= MASK; d2++) {
        res.add(DataStorage.BLOCK_SUBDIR_PREFIX + d1 + SEP +
            DataStorage.BLOCK_SUBDIR_PREFIX + d2);
      }
    }
    return res;
  }

  /**
   * 获取指定块元数据文件的文件输入流，用于读取块校验信息等元数据
   * @param b 目标扩展块对象
   * @param data 文件数据集实现
   * @return 元数据文件的输入流
   * @throws FileNotFoundException 元文件不存在抛出异常
   * @throws ClassCastException 底层流不是FileInputStream抛出异常
   */
  public static FileInputStream getMetaDataInputStream(
      ExtendedBlock b, FsDatasetSpi<?> data) throws IOException {
    final LengthInputStream lin = data.getMetaDataInputStream(b);
    if (lin == null) {
      throw new FileNotFoundException("Meta file for " + b + " not found.");
    }
    return (FileInputStream)lin.getWrappedStream();
  }
}