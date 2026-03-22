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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.util.StringUtils;

/**
 * 文件大小分布统计访问器，用于离线分析HDFS命名空间镜像中的文件大小分布情况。
 * 
 * <h3>Description.</h3>
 * This is the tool for analyzing file sizes in the namespace image.
 * In order to run the tool one should define a range of integers
 * <code>[0, maxSize]</code> by specifying <code>maxSize</code> and a <code>step</code>.
 * The range of integers is divided into segments of size <code>step</code>: 
 * <code>[0, s<sub>1</sub>, ..., s<sub>n-1</sub>, maxSize]</code>,
 * and the visitor calculates how many files in the system fall into 
 * each segment <code>[s<sub>i-1</sub>, s<sub>i</sub>)</code>. 
 * Note that files larger than <code>maxSize</code> always fall into 
 * the very last segment.
 * 
 * <h3>Input.</h3>
 * <ul>
 * <li><code>filename</code> specifies the location of the image file;</li>
 * <li><code>maxSize</code> determines the range <code>[0, maxSize]</code> of files
 * sizes considered by the visitor;</li>
 * <li><code>step</code> the range is divided into segments of size step.</li>
 * </ul>
 *
 * <h3>Output.</h3>
 * The output file is formatted as a tab separated two column table:
 * Size and NumFiles. Where Size represents the start of the segment,
 * and numFiles is the number of files form the image which size falls in 
 * this segment.
 */
/**
 * 遍历HDFS fsimage镜像文件，统计不同区间的文件数量，生成文件大小分布报告
 */
class FileDistributionVisitor extends TextWriterImageVisitor {
  // 存储解析元素的栈，用于维护嵌套元素的解析上下文
  final private LinkedList<ImageElement> elemS = new LinkedList<ImageElement>();

  // 默认最大统计文件大小，默认值为1/8 TB = 2^37字节
  private final static long MAX_SIZE_DEFAULT = 0x2000000000L;   // 1/8 TB = 2^37
  // 默认区间步长，默认值为2 MB = 2^21字节
  private final static int INTERVAL_DEFAULT = 0x200000;         // 2 MB = 2^21

  // 文件大小分布计数数组，每个位置对应一个区间的文件数量
  private int[] distribution;
  // 统计的最大文件大小边界
  private long maxSize;
  // 区间步长
  private int step;

  // 总文件数统计
  private int totalFiles;
  // 总目录数统计
  private int totalDirectories;
  // 总块数统计
  private int totalBlocks;
  // 总存储空间占用（包含副本）
  private long totalSpace;
  // 最大文件大小
  private long maxFileSize;

  // 当前正在解析的INode上下文
  private FileContext current;

  // 当前是否处于INode解析过程中
  private boolean inInode = false;
  // 是否输出格式化的人类可读区间范围
  private boolean formatOutput = false;

  /**
   * 存储单个文件/目录的上下文信息
   */
  private static class FileContext {
    String path;
    long fileSize;
    int numBlocks;
    int replication;
  }

  /**
   * 构造文件大小分布统计访问器
   * @param filename 输出结果文件名
   * @param maxSize 统计的最大文件大小边界，传入0则使用默认值
   * @param step 区间步长，传入0则使用默认值
   * @param formatOutput 是否格式化输出区间范围为人类可读格式
   * @throws IOException 初始化或输出文件创建失败时抛出异常
   */
  public FileDistributionVisitor(String filename, long maxSize, int step,
      boolean formatOutput) throws IOException {
    super(filename, false);
    this.maxSize = (maxSize == 0 ? MAX_SIZE_DEFAULT : maxSize);
    this.step = (step == 0 ? INTERVAL_DEFAULT : step);
    this.formatOutput = formatOutput;
    long numIntervals = this.maxSize / this.step;
    if(numIntervals >= Integer.MAX_VALUE)
      throw new IOException("Too many distribution intervals " + numIntervals);
    this.distribution = new int[1 + (int)(numIntervals)];
    this.totalFiles = 0;
    this.totalDirectories = 0;
    this.totalBlocks = 0;
    this.totalSpace = 0;
    this.maxFileSize = 0;
  }

  @Override
  void start() throws IOException {}

  @Override
  void finish() throws IOException {
    // 输出统计结果
    output();
    super.finish();
  }

  @Override
  void finishAbnormally() throws IOException {
    // 异常处理：打印提示信息后仍输出已统计结果
    System.out.println("*** Image processing finished abnormally.  Ending ***");
    output();
    super.finishAbnormally();
  }

  /**
   * 将统计结果输出到文件，并在控制台打印汇总统计信息
   * @throws IOException 写入输出文件失败时抛出异常
   */
  private void output() throws IOException {
    // write the distribution into the output file
    // 写入表头
    write((formatOutput ? "Size Range" : "Size") + "\tNumFiles\n");
    // 遍历每个分布区间，输出有文件的区间统计
    for (int i = 0; i < distribution.length; i++) {
      if (distribution[i] > 0) {
        if (formatOutput) {
          // 格式化输出区间范围为字节单位描述
          write((i == 0 ? "[" : "(")
              + StringUtils.byteDesc(((long) (i == 0 ? 0 : i - 1) * step))
              + ", "
              + StringUtils.byteDesc((long)
                  (i == distribution.length - 1 ? maxFileSize : i * step))
                  + "]\t"
              + distribution[i] + "\n");
        } else {
          // 输出原始字节起始位置
          write(((long) i * step) + "\t" + distribution[i] + "\n");
        }
      }
    }
    // 在控制台打印汇总统计信息
    System.out.println("totalFiles = " + totalFiles);
    System.out.println("totalDirectories = " + totalDirectories);
    System.out.println("totalBlocks = " + totalBlocks);
    System.out.println("totalSpace = " + totalSpace);
    System.out.println("maxFileSize = " + maxFileSize);
  }

  @Override
  void leaveEnclosingElement() throws IOException {
    // 弹出当前闭合元素
    ImageElement elem = elemS.pop();

    // 只有离开INode或构造中INode时才处理统计
    if(elem != ImageElement.INODE &&
       elem != ImageElement.INODE_UNDER_CONSTRUCTION)
      return;
    // 标记退出INode解析
    inInode = false;
    // numBlocks < 0 表示这是目录，增加目录计数后返回
    if(current.numBlocks < 0) {
      totalDirectories ++;
      return;
    }
    // 处理文件统计
    totalFiles++;
    totalBlocks += current.numBlocks;
    totalSpace += current.fileSize * current.replication;
    // 更新最大文件大小
    if(maxFileSize < current.fileSize)
      maxFileSize = current.fileSize;
    // 计算文件落在哪个分布区间
    int high;
    if(current.fileSize > maxSize)
      // 超过最大边界的文件统一落在最后一个区间
      high = distribution.length-1;
    else
      high = (int)Math.ceil((double)current.fileSize / step);

    // 边界防护：确保索引不越界
    if (high >= distribution.length) {
      high = distribution.length - 1;
    }
    // 对应区间计数加1
    distribution[high]++;
    // 每处理100万文件打印一次进度
    if(totalFiles % 1000000 == 1)
      System.out.println("Files processed: " + totalFiles
          + "  Current: " + current.path);
  }

  @Override
  void visit(ImageElement element, String value) throws IOException {
    // 只处理INode内部的属性
    if(inInode) {
      switch(element) {
      case INODE_PATH:
        // 设置文件路径，根路径特殊处理
        current.path = (value.equals("") ? "/" : value);
        break;
      case REPLICATION:
        // 解析副本数
        current.replication = Integer.parseInt(value);
        break;
      case NUM_BYTES:
        // 累加文件大小
        current.fileSize += Long.parseLong(value);
        break;
      default:
        break;
      }
    }
  }

  @Override
  void visitEnclosingElement(ImageElement element) throws IOException {
    // 元素入栈维护上下文
    elemS.push(element);
    // 进入INode节点，初始化上下文
    if(element == ImageElement.INODE ||
       element == ImageElement.INODE_UNDER_CONSTRUCTION) {
      current = new FileContext();
      inInode = true;
    }
  }

  @Override
  void visitEnclosingElement(ImageElement element,
      ImageElement key, String value) throws IOException {
    // 元素入栈维护上下文
    elemS.push(element);
    if(element == ImageElement.INODE ||
       element == ImageElement.INODE_UNDER_CONSTRUCTION)
      inInode = true;
    else if(element == ImageElement.BLOCKS)
      // 解析块数量
      current.numBlocks = Integer.parseInt(value);
  }
}