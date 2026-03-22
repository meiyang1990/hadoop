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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * TaskTracker节点信息数据容器，存储TaskTracker的基本信息以及黑名单状态相关数据，
 * 在MapReduce框架中用于JobTracker判断是否向该节点分配任务。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TaskTrackerInfo implements Writable {
  // TaskTracker节点名称
  String name;
  // 是否被列入黑名单
  boolean isBlacklisted = false;
  // 被加入黑名单的原因
  String reasonForBlacklist = "";
  // 黑名单详情报告
  String blacklistReport = "";
  
  /**
   * 空构造方法，用于反序列化创建对象。
   */
  public TaskTrackerInfo() {
  }

  /**
   * 构造可用TaskTracker信息，构造状态为非黑名单的正常节点信息。
   * @param name TaskTracker节点名称
   */
  public TaskTrackerInfo(String name) {
    this.name = name;
  }

  /**
   * 构造已被拉黑的TaskTracker信息，记录黑名单相关信息。
   * @param name TaskTracker节点名称
   * @param reasonForBlacklist 被拉黑的原因
   * @param report 黑名单详情报告
   */
  public TaskTrackerInfo(String name, String reasonForBlacklist,
      String report) {
    this.name = name;
    this.isBlacklisted = true;
    this.reasonForBlacklist = reasonForBlacklist;
    this.blacklistReport = report;
  }

  /**
   * 获取TaskTracker节点名称。
   * 
   * @return TaskTracker节点名称
   */
  public String getTaskTrackerName() {
    return name;
  }
  
  /**
   * 判断当前TaskTracker是否被列入黑名单（黑名单节点不会分配新任务）。
   * @return true表示已被拉黑，false表示节点正常可用
   */
  public boolean isBlacklisted() {
    return isBlacklisted;
  }
  
  /**
   * 获取TaskTracker被列入黑名单的原因。
   * 
   * @return 被拉黑原因描述
   */
  public String getReasonForBlacklist() {
    return reasonForBlacklist;
  }

  /**
   * 获取TaskTracker被列入黑名单的详细描述报告。
   * 
   * @return 黑名单详情报告文本
   */
  public String getBlacklistReport() {
    return blacklistReport;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // 反序列化读取节点名称
    name = Text.readString(in);
    // 反序列化读取黑名单标记
    isBlacklisted = in.readBoolean();
    // 反序列化读取拉黑原因
    reasonForBlacklist = Text.readString(in);
    // 反序列化读取黑名单报告
    blacklistReport = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 序列化输出节点名称
    Text.writeString(out, name);
    // 序列化输出黑名单标记
    out.writeBoolean(isBlacklisted);
    // 序列化输出拉黑原因
    Text.writeString(out, reasonForBlacklist);
    // 序列化输出黑名单报告
    Text.writeString(out, blacklistReport);
  }

}