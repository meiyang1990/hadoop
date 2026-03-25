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
package org.apache.hadoop.mapreduce.checkpoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;

/**
 * MapReduce任务检查点ID实现类，保存任务检查点相关的元数据信息。
 * 该类包含基于HDFS文件系统的检查点位置引用、检查点开销统计信息以及任务计数器，
 * 由任务发送给ApplicationMaster保存，供同一任务下次执行时恢复使用。
 */
public class TaskCheckpointID implements CheckpointID {

  // 基础文件系统检查点ID
  final FSCheckpointID rawId;
  // 已生成的部分输出文件路径列表
  private final List<Path> partialOutput;
  // 任务运行计数器集合
  private final Counters counters;

  /**
   * 空构造函数，初始化默认值
   */
  public TaskCheckpointID() {
    this(new FSCheckpointID(), new ArrayList<Path>(), new Counters());
  }

  /**
   * 构造函数，使用指定参数创建任务检查点ID
   * @param rawId 基础文件系统检查点ID
   * @param partialOutput 部分输出文件路径列表
   * @param counters 任务计数器集合
   */
  public TaskCheckpointID(FSCheckpointID rawId, List<Path> partialOutput,
          Counters counters) {
    this.rawId = rawId;
    this.counters = counters;
    this.partialOutput = null == partialOutput
      ? new ArrayList<Path>()
      : partialOutput;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 写入计数器
    counters.write(out);
    // 写入部分输出路径数量
    WritableUtils.writeVLong(out, partialOutput.size());
    // 逐个写入部分输出路径字符串
    for (Path p : partialOutput) {
      Text.writeString(out, p.toString());
    }
    // 写入基础检查点ID
    rawId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // 清空已有部分输出路径
    partialOutput.clear();
    // 读取计数器
    counters.readFields(in);
    // 读取部分输出路径数量
    long numPout = WritableUtils.readVLong(in);
    // 逐个读取部分输出路径
    for (int i = 0; i < numPout; i++) {
      partialOutput.add(new Path(Text.readString(in)));
    }
    // 读取基础检查点ID
    rawId.readFields(in);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TaskCheckpointID){
      TaskCheckpointID o = (TaskCheckpointID) other;
      // 需要基础ID、计数器、部分输出路径集合全部相等才判定为相等
      return rawId.equals(o.rawId) &&
             counters.equals(o.counters) &&
             partialOutput.containsAll(o.partialOutput) &&
             o.partialOutput.containsAll(partialOutput);
    }
    return false;
  }

  @Override
  public int hashCode() {
    // 使用基础检查点ID的哈希值作为本对象的哈希值
    return rawId.hashCode();
  }

  /**
   * 获取检查点的总字节大小
   * @return 检查点字节数
   */
  public long getCheckpointBytes() {
    return counters.findCounter(EnumCounter.CHECKPOINT_BYTES).getValue();
  }

  /**
   * 获取生成该检查点消耗的总时间
   * @return 检查点生成耗时（毫秒）
   */
  public long getCheckpointTime() {
    return counters.findCounter(EnumCounter.CHECKPOINT_MS).getValue();
  }

  @Override
  public String toString() {
    return rawId.toString() + " counters:" + counters;

  }

  /**
   * 获取检查点包含的已提交部分输出路径列表
   * @return 部分输出路径列表
   */
  public List<Path> getPartialCommittedOutput() {
    return partialOutput;
  }

  /**
   * 获取检查点保存的任务计数器
   * @return 计数器集合
   */
  public Counters getCounters() {
    return counters;
  }

}