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
package org.apache.hadoop.mapreduce.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件级说明：MapReduce作业分片信息的基础容器类，将分片信息分为元信息和原始分片数据两部分，
 * 分别服务于作业初始化阶段的资源调度和Map任务运行阶段的数据读取。
 * 元信息写入单独元文件供JobTracker加载，原始分片数据存储在独立文件中，由Map任务运行时读取。
 *
 * This class groups the fundamental classes associated with
 * reading/writing splits. The split information is divided into
 * two parts based on the consumer of the information. The two
 * parts are the split meta information, and the raw split 
 * information. The first part is consumed by the JobTracker to
 * create the tasks' locality data structures. The second part is
 * used by the maps at runtime to know what to do!
 * These pieces of information are written to two separate files.
 * The metainformation file is slurped by the JobTracker during 
 * job initialization. A map task gets the meta information during
 * the launch and it reads the raw split bytes directly from the 
 * file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * JobSplit 容器类，聚合了所有和作业分片相关的内部类，定义了分片信息的存储结构
 */
public class JobSplit {
  // 元分片文件版本号
  static final int META_SPLIT_VERSION = 1;
  // 元分片文件文件头标识
  static final byte[] META_SPLIT_FILE_HEADER = "META-SPL".getBytes(StandardCharsets.UTF_8);
  // 空任务分片元信息实例，作为默认值使用
  public static final TaskSplitMetaInfo EMPTY_TASK_SPLIT =
    new TaskSplitMetaInfo();
  
  /**
   * 分片元信息类，存储单个分片的核心调度信息：偏移量、数据长度、数据所在节点位置
   * 供JobTracker构建任务数据局部性调度结构使用，实现Writable支持序列化
   */
  public static class SplitMetaInfo implements Writable {
    private long startOffset;
    private long inputDataLength;
    private String[] locations;

    public SplitMetaInfo() {}
    
    /**
     * 构造方法，通过分片位置、偏移量、数据长度构造分片元信息
     * @param locations 该分片数据所在的节点主机列表
     * @param startOffset 原始分片文件中的起始偏移量
     * @param inputDataLength 该分片的数据总长度
     */
    public SplitMetaInfo(String[] locations, long startOffset, 
        long inputDataLength) {
      this.locations = locations;
      this.startOffset = startOffset;
      this.inputDataLength = inputDataLength;
    }
    
    /**
     * 构造方法，从InputSplit对象和起始偏移量构造分片元信息
     * @param split 原始InputSplit对象
     * @param startOffset 原始分片文件中的起始偏移量
     * @throws IOException 中断异常封装为IOException抛出
     */
    public SplitMetaInfo(InputSplit split, long startOffset) throws IOException {
      try {
        this.locations = split.getLocations();
        this.inputDataLength = split.getLength();
        this.startOffset = startOffset;
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    
    public String[] getLocations() {
      return locations;
    }
  
    public long getStartOffset() {
      return startOffset;
    }
      
    public long getInputDataLength() {
      return inputDataLength;
    }
    
    public void setInputDataLocations(String[] locations) {
      this.locations = locations;
    }
    
    public void setInputDataLength(long length) {
      this.inputDataLength = length;
    }
    
    /**
     * 从输入流反序列化读取分片元信息
     * @param in 数据输入流
     * @throws IOException 读取异常
     */
    public void readFields(DataInput in) throws IOException {
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = Text.readString(in);
      }
      startOffset = WritableUtils.readVLong(in);
      inputDataLength = WritableUtils.readVLong(in);
    }
  
    /**
     * 将分片元信息序列化写入输出流
     * @param out 数据输出流
     * @throws IOException 写入异常
     */
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVInt(out, locations.length);
      for (int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }
      WritableUtils.writeVLong(out, startOffset);
      WritableUtils.writeVLong(out, inputDataLength);
    }
    
    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("data-size : " + inputDataLength + "\n");
      buf.append("start-offset : " + startOffset + "\n");
      buf.append("locations : " + "\n");
      for (String loc : locations) {
        buf.append("  " + loc + "\n");
      }
      return buf.toString();
    }
  }

  /**
   * 任务分片元信息，JobTracker创建任务时使用，包含分片索引、数据长度、位置信息
   */
  public static class TaskSplitMetaInfo {
    private TaskSplitIndex splitIndex;
    private long inputDataLength;
    private String[] locations;
    public TaskSplitMetaInfo(){
      this.splitIndex = new TaskSplitIndex();
      this.locations = new String[0];
    }

    /**
     * 构造方法，通过分片索引、位置列表、数据长度构造任务分片元信息
     * @param splitIndex 分片索引，指向原始分片数据位置
     * @param locations 数据所在节点主机列表
     * @param inputDataLength 分片数据长度
     */
    public TaskSplitMetaInfo(TaskSplitIndex splitIndex, String[] locations, 
        long inputDataLength) {
      this.splitIndex = splitIndex;
      this.locations = locations;
      this.inputDataLength = inputDataLength;
    }

    /**
     * 构造方法，从InputSplit和起始偏移量构造任务分片元信息
     * @param split 原始InputSplit对象
     * @param startOffset 原始分片文件中的起始偏移量
     * @throws InterruptedException 获取位置信息时的中断异常
     * @throws IOException IO异常
     */
    public TaskSplitMetaInfo(InputSplit split, long startOffset) 
    throws InterruptedException, IOException {
      this(new TaskSplitIndex("", startOffset), split.getLocations(), 
          split.getLength());
    }
    
    /**
     * 构造方法，通过位置列表、起始偏移量、数据长度构造任务分片元信息
     * @param locations 数据所在节点主机列表
     * @param startOffset 原始分片文件中的起始偏移量
     * @param inputDataLength 分片数据长度
     */
    public TaskSplitMetaInfo(String[] locations, long startOffset, 
        long inputDataLength) {
      this(new TaskSplitIndex("",startOffset), locations, inputDataLength);
    }
    
    public TaskSplitIndex getSplitIndex() {
      return splitIndex;
    }
    
    public String getSplitLocation() {
      return splitIndex.getSplitLocation();
    }
    public long getInputDataLength() {
      return inputDataLength;
    }
    public String[] getLocations() {
      return locations;
    }
    public long getStartOffset() {
      return splitIndex.getStartOffset();
    }
  }
  
  /**
   * 分片索引，任务获取分片时使用，存储原始分片数据的文件路径和偏移量
   */
  public static class TaskSplitIndex {
    private String splitLocation;
    private long startOffset;
    public TaskSplitIndex(){
      this("", 0);
    }

    /**
     * 构造方法，通过分片文件路径和起始偏移量构造分片索引
     * @param splitLocation 原始分片文件路径
     * @param startOffset 原始分片文件中的起始偏移量
     */
    public TaskSplitIndex(String splitLocation, long startOffset) {
      this.splitLocation = splitLocation;
      this.startOffset = startOffset;
    }
    public long getStartOffset() {
      return startOffset;
    }
    public String getSplitLocation() {
      return splitLocation;
    }

    /**
     * 从输入流反序列化读取分片索引
     * @param in 数据输入流
     * @throws IOException 读取异常
     */
    public void readFields(DataInput in) throws IOException {
      splitLocation = Text.readString(in);
      startOffset = WritableUtils.readVLong(in);
    }

    /**
     * 将分片索引序列化写入输出流
     * @param out 数据输出流
     * @throws IOException 写入异常
     */
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitLocation);
      WritableUtils.writeVLong(out, startOffset);
    }
  }
}