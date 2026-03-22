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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件大小分布统计工具，用于分析HDFS命名空间镜像中的文件大小分布情况。
 * 通过指定最大文件大小和步长，将文件大小范围划分成多个区间，统计每个区间内的文件数量。
 * 大于最大文件大小的文件统一计入最后一个区间，最终输出制表符分隔的统计结果。
 */
final class FileDistributionCalculator {
  private final static long MAX_SIZE_DEFAULT = 0x2000000000L; // 1/8 TB = 2^37
  private final static int INTERVAL_DEFAULT = 0x200000; // 2 MB = 2^21
  private final static int MAX_INTERVALS = 0x8000000; // 128 M = 2^27

  private final Configuration conf;
  private final long maxSize;
  private final int steps;
  private final PrintStream out;

  private final int[] distribution;
  private int totalFiles;
  private int totalDirectories;
  private int totalBlocks;
  private long totalSpace;
  private long maxFileSize;

  private boolean formatOutput = false;

  /**
   * 构造文件分布计算器，初始化参数和统计数组
   * @param conf Hadoop配置对象
   * @param maxSize 统计的最大文件大小，0则使用默认值
   * @param steps 每个区间的步长，0则使用默认值
   * @param formatOutput 是否格式化输出区间描述
   * @param out 输出流用于输出统计结果
   */
  FileDistributionCalculator(Configuration conf, long maxSize, int steps,
      boolean formatOutput, PrintStream out) {
    this.conf = conf;
    this.maxSize = maxSize == 0 ? MAX_SIZE_DEFAULT : maxSize;
    this.steps = steps == 0 ? INTERVAL_DEFAULT : steps;
    this.formatOutput = formatOutput;
    this.out = out;
    long numIntervals = this.maxSize / this.steps;
    // 避免分配过大数组导致OOM，检查区间数量不超过上限
    Preconditions.checkState(numIntervals <= MAX_INTERVALS,
        "Too many distribution intervals (maxSize/step): " + numIntervals +
        ", should be less than " + (MAX_INTERVALS+1) + ".");
    this.distribution = new int[1 + (int) (numIntervals)];
  }

  /**
   * 从FSImage文件中读取INode信息，统计文件大小分布
   * @param file 打开的FSImage随机访问文件
   * @throws IOException 读取文件或解析错误时抛出
   */
  void visit(RandomAccessFile file) throws IOException {
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);
    try (FileInputStream in = new FileInputStream(file.getFD())) {
      // 遍历所有段，找到INode段进行处理
      for (FileSummary.Section s : summary.getSectionsList()) {
        if (SectionName.fromString(s.getName()) != SectionName.INODE) {
          continue;
        }
        // 定位到INode段偏移
        in.getChannel().position(s.getOffset());
        // 包装压缩输入流，限定位移不超过段长度
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                in, s.getLength())));
        // 运行统计
        run(is);
        // 输出结果
        output();
      }
    }
  }

  /**
   * 解析INode段输入流，统计文件大小分布
   * @param in INode段输入流
   * @throws IOException 解析错误时抛出
   */
  private void run(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    // 遍历所有INode
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
      if (p.getType() == INodeSection.INode.Type.FILE) {
        // 统计文件总数
        ++totalFiles;
        INodeSection.INodeFile f = p.getFile();
        // 统计块总数
        totalBlocks += f.getBlocksCount();
        // 累加计算文件总大小
        long fileSize = 0;
        for (BlockProto b : f.getBlocksList()) {
          fileSize += b.getNumBytes();
        }
        // 更新最大文件大小
        maxFileSize = Math.max(fileSize, maxFileSize);
        // 累加总空间占用（考虑副本数）
        totalSpace += fileSize * f.getReplication();

        // 计算当前文件所属区间
        int bucket = fileSize > maxSize ? distribution.length - 1 : (int) Math
            .ceil((double)fileSize / steps);
        // 边界检查：当maxSize无法被步长整除时，bucket可能等于数组长度，需要修正到最后一位
        if (bucket >= distribution.length) {
          bucket = distribution.length - 1;
        }
        // 当前区间计数+1
        ++distribution[bucket];

      } else if (p.getType() == INodeSection.INode.Type.DIRECTORY) {
        // 统计目录总数
        ++totalDirectories;
      }

      // 每处理100万个INode输出进度提示
      if (i % (1 << 20) == 0) {
        out.println("Processed " + i + " inodes.");
      }
    }
  }

  /**
   * 输出文件大小分布统计结果和汇总信息
   */
  private void output() {
    // 输出表头
    out.print((formatOutput ? "Size Range" : "Size") + "\tNumFiles\n");
    // 遍历每个区间，输出非零区间的统计结果
    for (int i = 0; i < distribution.length; i++) {
      if (distribution[i] != 0) {
        if (formatOutput) {
          // 格式化输出，显示区间范围
          out.print((i == 0 ? "[" : "(")
              + StringUtils.byteDesc(((long) (i == 0 ? 0 : i - 1) * steps))
              + ", "
              + StringUtils.byteDesc((long)
                  (i == distribution.length - 1 ? maxFileSize :
                      (long) i * steps)) + "]\t" + distribution[i]);
        } else {
          // 简单输出区间起始位置和文件数量
          out.print(((long) i * steps) + "\t" + distribution[i]);
        }

        out.print('\n');
      }
    }
    // 输出汇总统计信息
    out.print("totalFiles = " + totalFiles + "\n");
    out.print("totalDirectories = " + totalDirectories + "\n");
    out.print("totalBlocks = " + totalBlocks + "\n");
    out.print("totalSpace = " + totalSpace + "\n");
    out.print("maxFileSize = " + maxFileSize + "\n");
  }
}