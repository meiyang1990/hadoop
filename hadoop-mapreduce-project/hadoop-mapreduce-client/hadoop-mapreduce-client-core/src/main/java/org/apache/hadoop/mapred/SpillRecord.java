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
package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.util.PureJavaCrc32;

/**
 * Map任务溢写索引记录，存储Map输出不同分区在溢写文件中的偏移信息，用于Shuffle阶段读取分区数据。
 * 每个溢写文件对应一个SpillRecord，保存所有分区的起始偏移、原始长度和压缩后长度信息。
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class SpillRecord {

  /** Backing store */
  private final ByteBuffer buf;
  /** View of backing storage as longs */
  private final LongBuffer entries;

  /**
   * 构造指定分区数量的空SpillRecord，用于写入新的溢写索引
   * @param numPartitions 分区数量
   */
  public SpillRecord(int numPartitions) {
    buf = ByteBuffer.allocate(
        numPartitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
    entries = buf.asLongBuffer();
  }

  /**
   * 从指定索引文件加载SpillRecord，使用默认CRC32校验
   * @param indexFileName 溢写索引文件路径
   * @param job 作业配置对象
   * @throws IOException 读取索引文件失败时抛出
   */
  public SpillRecord(Path indexFileName, JobConf job) throws IOException {
    this(indexFileName, job, null);
  }

  /**
   * 从指定索引文件加载SpillRecord，校验文件所有者并使用默认CRC32校验
   * @param indexFileName 溢写索引文件路径
   * @param job 作业配置对象
   * @param expectedIndexOwner 预期的文件所有者，用于安全校验
   * @throws IOException 读取索引文件失败时抛出
   */
  public SpillRecord(Path indexFileName, JobConf job, String expectedIndexOwner)
    throws IOException {
    this(indexFileName, job, new PureJavaCrc32(), expectedIndexOwner);
  }

  /**
   * 从指定索引文件加载SpillRecord，支持自定义校验和
   * @param indexFileName 溢写索引文件路径
   * @param job 作业配置对象
   * @param crc 校验和对象
   * @param expectedIndexOwner 预期的文件所有者，用于安全校验
   * @throws IOException 读取索引文件失败或校验错误时抛出
   */
  public SpillRecord(Path indexFileName, JobConf job, Checksum crc,
                     String expectedIndexOwner)
      throws IOException {

    // 获取本地文件系统原始接口
    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    // 安全打开输入流，校验文件所有者
    final FSDataInputStream in =
        SecureIOUtils.openFSDataInputStream(new File(indexFileName.toUri()
            .getRawPath()), expectedIndexOwner, null);
    try {
      // 获取索引文件总长度
      final long length = rfs.getFileStatus(indexFileName).getLen();
      // 计算分区数量
      final int partitions = (int) length / MAP_OUTPUT_INDEX_RECORD_LENGTH;
      // 计算索引数据总大小
      final int size = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      // 分配缓冲区存储索引数据
      buf = ByteBuffer.allocate(size);
      // 若提供了校验对象，进行校验读取
      if (crc != null) {
        crc.reset();
        CheckedInputStream chk = new CheckedInputStream(in, crc);
        IOUtils.readFully(chk, buf.array(), 0, size);
        
        // 验证校验和，不匹配则抛出异常
        if (chk.getChecksum().getValue() != in.readLong()) {
          throw new ChecksumException("Checksum error reading spill index: " +
                                indexFileName, -1);
        }
      } else {
        // 无校验，直接读取索引数据
        IOUtils.readFully(in, buf.array(), 0, size);
      }
      // 转换为LongBuffer方便按长整型读取索引项
      entries = buf.asLongBuffer();
    } finally {
      // 关闭输入流
      in.close();
    }
  }

  /**
   * 返回当前溢写记录包含的索引项总数（即分区数）
   * @return 索引项数量
   */
  public int size() {
    return entries.capacity() / (MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
  }

  /**
   * 获取指定分区的溢写索引信息
   * @param partition 分区编号
   * @return 对应分区的索引记录，包含偏移和长度信息
   */
  public IndexRecord getIndex(int partition) {
    final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    return new IndexRecord(entries.get(pos), entries.get(pos + 1),
                           entries.get(pos + 2));
  }

  /**
   * 将指定分区的索引信息写入溢写记录
   * @param rec 分区索引记录
   * @param partition 分区编号
   */
  public void putIndex(IndexRecord rec, int partition) {
    final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    entries.put(pos, rec.startOffset);
    entries.put(pos + 1, rec.rawLength);
    entries.put(pos + 2, rec.partLength);
  }

  /**
   * 将当前溢写索引写入文件，使用默认CRC32校验
   * @param loc 输出索引文件路径
   * @param job 作业配置对象
   * @throws IOException 写入文件失败时抛出
   */
  public void writeToFile(Path loc, JobConf job)
      throws IOException {
    writeToFile(loc, job, new PureJavaCrc32());
  }

  /**
   * 将当前溢写索引写入文件，支持自定义校验和
   * @param loc 输出索引文件路径
   * @param job 作业配置对象
   * @param crc 校验和对象
   * @throws IOException 写入文件失败时抛出
   */
  public void writeToFile(Path loc, JobConf job, Checksum crc)
      throws IOException {
    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    CheckedOutputStream chk = null;
    // 创建输出流
    final FSDataOutputStream out = rfs.create(loc);
    try {
      // 若提供了校验对象，计算校验和写入
      if (crc != null) {
        crc.reset();
        chk = new CheckedOutputStream(out, crc);
        chk.write(buf.array());
        out.writeLong(chk.getChecksum().getValue());
      } else {
        // 无校验，直接写入索引数据
        out.write(buf.array());
      }
    } finally {
      // 关闭流
      if (chk != null) {
        chk.close();
      } else {
        out.close();
      }
    }
  }

}