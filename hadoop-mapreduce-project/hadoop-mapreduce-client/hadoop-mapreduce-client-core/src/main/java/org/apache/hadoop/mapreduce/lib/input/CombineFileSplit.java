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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * 文件输入分片合并实现，将多个小文件/文件块组合为一个分片
 * 
 * 与{@link FileSplit}不同，该类不代表单个文件的分片，而是将多个输入文件组合为一个分片。
 * 一个分片可以包含来自不同文件的块，且同一分片中的块通常位于同一机架，提升数据本地性。
 * <br>CombineFileSplit可用于实现一次读取一个文件的{@link RecordReader}，适合处理大量小文件场景。
 * 
 * @see FileSplit
 * @see CombineFileInputFormat 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineFileSplit extends InputSplit implements Writable {

  private Path[] paths;
  private long[] startoffset;
  private long[] lengths;
  private String[] locations;
  private long totLength;

  /**
   * 默认构造函数，用于反序列化
   */
  public CombineFileSplit() {}

  /**
   * 构造合并分片，指定所有文件信息和节点位置
   * @param files 分片包含的文件路径数组
   * @param start 每个文件的起始偏移量数组
   * @param lengths 每个文件的长度数组
   * @param locations 每个文件所在的数据节点位置数组
   */
  public CombineFileSplit(Path[] files, long[] start, 
                          long[] lengths, String[] locations) {
    initSplit(files, start, lengths, locations);
  }

  /**
   * 构造合并分片，起始偏移量默认为0，位置为空
   * @param files 分片包含的文件路径数组
   * @param lengths 每个文件的长度数组
   */
  public CombineFileSplit(Path[] files, long[] lengths) {
    long[] startoffset = new long[files.length];
    for (int i = 0; i < startoffset.length; i++) {
      startoffset[i] = 0;
    }
    String[] locations = new String[files.length];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = "";
    }
    initSplit(files, startoffset, lengths, locations);
  }
  
  /**
   * 初始化合并分片，计算总长度
   */
  private void initSplit(Path[] files, long[] start, 
                         long[] lengths, String[] locations) {
    this.startoffset = start;
    this.lengths = lengths;
    this.paths = files;
    this.totLength = 0;
    this.locations = locations;
    for(long length : lengths) {
      totLength += length;
    }
  }

  /**
   * 拷贝构造函数，从已有合并分片创建新分片
   * @param old 原有合并分片
   * @throws IOException
   */
  public CombineFileSplit(CombineFileSplit old) throws IOException {
    this(old.getPaths(), old.getStartOffsets(),
         old.getLengths(), old.getLocations());
  }

  @Override
  public long getLength() {
    return totLength;
  }

  /** 获取分片中所有文件的起始偏移量数组 */ 
  public long[] getStartOffsets() {
    return startoffset;
  }
  
  /** 获取分片中所有文件的长度数组 */ 
  public long[] getLengths() {
    return lengths;
  }

  /** 获取第i个文件的起始偏移量 */
  public long getOffset(int i) {
    return startoffset[i];
  }
  
  /** 获取第i个文件的长度 */
  public long getLength(int i) {
    return lengths[i];
  }
  
  /** 获取分片包含的文件数量 */
  public int getNumPaths() {
    return paths.length;
  }

  /** 获取第i个文件的路径 */
  public Path getPath(int i) {
    return paths[i];
  }
  
  /** 获取分片中所有文件的路径数组 */
  public Path[] getPaths() {
    return paths;
  }

  @Override
  public String[] getLocations() throws IOException {
    return locations;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取总长度
    totLength = in.readLong();
    // 读取长度数组
    int arrLength = in.readInt();
    lengths = new long[arrLength];
    for(int i=0; i<arrLength;i++) {
      lengths[i] = in.readLong();
    }
    // 读取文件路径数组
    int filesLength = in.readInt();
    paths = new Path[filesLength];
    for(int i=0; i<filesLength;i++) {
      paths[i] = new Path(Text.readString(in));
    }
    // 读取起始偏移量数组
    arrLength = in.readInt();
    startoffset = new long[arrLength];
    for(int i=0; i<arrLength;i++) {
      startoffset[i] = in.readLong();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 写入总长度
    out.writeLong(totLength);
    // 写入长度数组
    out.writeInt(lengths.length);
    for(long length : lengths) {
      out.writeLong(length);
    }
    // 写入文件路径数组
    out.writeInt(paths.length);
    for(Path p : paths) {
      Text.writeString(out, p.toString());
    }
    // 写入起始偏移量数组
    out.writeInt(startoffset.length);
    for(long length : startoffset) {
      out.writeLong(length);
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    // 拼接所有文件信息
    for (int i = 0; i < paths.length; i++) {
      if (i == 0 ) {
        sb.append("Paths:");
      }
      sb.append(paths[i].toUri().getPath() + ":" + startoffset[i] +
                "+" + lengths[i]);
      if (i < paths.length -1) {
        sb.append(",");
      }
    }
    // 拼接节点位置信息
    if (locations != null) {
      String locs = "";
      StringBuilder locsb = new StringBuilder();
      for (int i = 0; i < locations.length; i++) {
        locsb.append(locations[i] + ":");
      }
      locs = locsb.toString();
      sb.append(" Locations:" + locs + "; ");
    }
    return sb.toString();
  }
}