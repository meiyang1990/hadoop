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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

/**
 * HDFS DataNode FsDataset实现的工具类，提供块文件、元数据文件处理、校验和计算等通用工具方法
 */
@InterfaceAudience.Private
public class FsDatasetUtil {
  /**
   * 判断文件是否是未链接块临时文件
   * @param f 待判断的文件
   * @return 是否为未链接临时文件
   */
  static boolean isUnlinkTmpFile(File f) {
    return f.getName().endsWith(DatanodeUtil.UNLINK_BLOCK_SUFFIX);
  }

  /**
   * 创建空校验和对应的元数据头字节数组，用于不需要校验和的场景
   * @return 空校验和元数据头字节数组
   */
  public static byte[] createNullChecksumByteArray() {
    DataChecksum csum =
        DataChecksum.newDataChecksum(DataChecksum.Type.NULL, 512);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      BlockMetadataHeader.writeHeader(dataOut, csum);
      dataOut.close();
    } catch (IOException e) {
      FsVolumeImpl.LOG.error(
          "Exception in creating null checksum stream: " + e);
      return null;
    }
    return out.toByteArray();
  }

  /**
   * 从未链接临时文件名获取原块文件对象
   * @param unlinkTmpFile 未链接临时文件
   * @return 原块文件对象
   */
  static File getOrigFile(File unlinkTmpFile) {
    final String name = unlinkTmpFile.getName();
    if (!name.endsWith(DatanodeUtil.UNLINK_BLOCK_SUFFIX)) {
      throw new IllegalArgumentException("unlinkTmpFile=" + unlinkTmpFile
          + " does not end with " + DatanodeUtil.UNLINK_BLOCK_SUFFIX);
    }
    final int n = name.length() - DatanodeUtil.UNLINK_BLOCK_SUFFIX.length(); 
    return new File(unlinkTmpFile.getParentFile(), name.substring(0, n));
  }
  
  /**
   * 根据块文件和生成时间戳获取元数据文件对象
   * @param f 块文件
   * @param gs 块生成时间戳
   * @return 元数据文件对象
   */
  static File getMetaFile(File f, long gs) {
    return new File(f.getParent(),
        DatanodeUtil.getMetaName(f.getName(), gs));
  }

  /**
   * 根据给定块文件查找对应的元数据文件
   * @param blockFile 块数据文件
   * @return 匹配的元数据文件
   * @throws IOException 找不到或找到多个元数据文件时抛出异常
   */
  public static File findMetaFile(final File blockFile) throws IOException {
    final String prefix = blockFile.getName() + "_";
    final File parent = blockFile.getParentFile();
    final File[] matches = parent.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return dir.equals(parent) && name.startsWith(prefix)
            && name.endsWith(Block.METADATA_EXTENSION);
      }
    });

    if (matches == null || matches.length == 0) {
      throw new FileNotFoundException(
          "Meta file not found, blockFile=" + blockFile);
    }
    if (matches.length > 1) {
      throw new IOException("Found more than one meta files: " 
          + Arrays.asList(matches));
    }
    return matches[0];
  }

  /**
   * 打开指定文件并将文件指针偏移到指定位置，返回文件描述符
   * @param file 待打开的文件
   * @param offset 需要偏移到的位置
   * @return 偏移后的文件描述符
   * @throws IOException 打开或偏移失败时抛出异常
   */
  public static FileDescriptor openAndSeek(File file, long offset)
      throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(file, "r");
      if (offset > 0) {
        raf.seek(offset);
      }
      return raf.getFD();
    } catch(IOException ioe) {
      IOUtils.cleanupWithLogger(null, raf);
      throw ioe;
    }
  }

  /**
   * 打开指定文件并偏移到指定位置，返回输入流
   * @param file 待打开的文件
   * @param offset 需要偏移到的位置
   * @return 包装后的输入流
   * @throws IOException 打开或偏移失败时抛出异常
   */
  public static InputStream getInputStreamAndSeek(File file, long offset)
      throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(file, "r");
      raf.seek(offset);
      return Channels.newInputStream(raf.getChannel());
    } catch(IOException ioe) {
      IOUtils.cleanupWithLogger(null, raf);
      throw ioe;
    }
  }

  /**
   * 通过反射构造直接ByteBuffer，并基于内存地址获取输入流，用于直接访问堆外内存
   * @param addr 堆外内存起始地址
   * @param length 内存长度
   * @return 包装后输入流
   * @throws IOException 反射构造失败时抛出异常
   */
  public static InputStream getDirectInputStream(long addr, long length)
      throws IOException {
    try {
      Class<?> directByteBufferClass =
          Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor =
          directByteBufferClass.getDeclaredConstructor(long.class, int.class);
      constructor.setAccessible(true);
      ByteBuffer byteBuffer =
          (ByteBuffer) constructor.newInstance(addr, (int)length);
      return new ByteBufferBackedInputStream(byteBuffer);
    } catch (ClassNotFoundException | NoSuchMethodException |
        IllegalAccessException | InvocationTargetException |
        InstantiationException e) {
      throw new IOException(e);
    }
  }

  /**
   * 从已排序的文件列表中，根据块文件位置获取下一个元数据文件中的生成时间戳
   * @param listdir 按名称排序的文件数组
   * @param blockFile 目标块文件
   * @param index 块文件在数组中的索引
   * @return 块的生成时间戳
   */
  static long getGenerationStampFromFile(File[] listdir, File blockFile,
      int index) {
    String blockName = blockFile.getName();
    if ((index + 1) < listdir.length) {
      // Check if next index file is meta file
      String metaFile = listdir[index + 1].getName();
      if (metaFile.startsWith(blockName)) {
        return Block.getGenerationStamp(metaFile);
      }
    }
    FsDatasetImpl.LOG.warn("Block " + blockFile + " does not have a metafile!");
    return HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }

  /**
   * 从元数据文件名中解析得到块生成时间戳
   * @param blockFile 块数据文件
   * @param metaFile 块元数据文件
   * @return 解析得到的生成时间戳
   * @throws IOException 解析失败时抛出异常
   */
  static long parseGenerationStamp(File blockFile, File metaFile
      ) throws IOException {
    final String metaname = metaFile.getName();
    final String gs = metaname.substring(blockFile.getName().length() + 1,
        metaname.length() - Block.METADATA_EXTENSION.length());
    try {
      return Long.parseLong(gs);
    } catch(NumberFormatException nfe) {
      throw new IOException("Failed to parse generation stamp: blockFile="
          + blockFile + ", metaFile=" + metaFile, nfe);
    }
  }

  /**
   * 为还未计算校验和的块文件计算校验和，并保存到目标元数据文件
   * @param srcMeta 源元数据文件
   * @param dstMeta 目标元数据文件，用于保存计算后的校验和
   * @param blockFile 块数据文件
   * @param smallBufferSize 缓冲区大小
   * @param conf Hadoop配置对象
   * @throws IOException 计算过程中IO异常抛出
   */
  public static void computeChecksum(File srcMeta, File dstMeta,
      File blockFile, int smallBufferSize, Configuration conf)
          throws IOException {
    Preconditions.checkNotNull(srcMeta);
    Preconditions.checkNotNull(dstMeta);
    Preconditions.checkNotNull(blockFile);
    // 创建一个匿名ReplicaInfo封装块文件和元数据路径，供校验和计算使用
    ReplicaInfo wrapper = new FinalizedReplica(0, 0, 0, null, null) {
      @Override
      public URI getMetadataURI() {
        return srcMeta.toURI();
      }

      @Override
      public InputStream getDataInputStream(long seekOffset)
          throws IOException {
        return Files.newInputStream(blockFile.toPath());
      }
    };

    FsDatasetImpl.computeChecksum(wrapper, dstMeta, smallBufferSize, conf);
  }

  /**
   * 删除指定路径的内存映射文件
   * @param filePath 待删除文件路径
   * @throws IOException 删除失败或路径为空时抛出异常
   */
  public static void deleteMappedFile(String filePath) throws IOException {
    if (filePath == null) {
      throw new IOException("The filePath should not be null!");
    }
    boolean result = Files.deleteIfExists(Paths.get(filePath));
    if (!result) {
      throw new IOException(
          "Failed to delete the mapped file: " + filePath);
    }
  }
}