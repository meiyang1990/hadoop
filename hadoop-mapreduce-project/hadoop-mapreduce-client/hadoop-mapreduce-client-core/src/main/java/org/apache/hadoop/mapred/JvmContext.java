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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM进程上下文信息类，用于在MapReduce任务和TaskTracker之间传递JVM标识信息，
 * 支持Hadoop Writable序列化机制，实现JVM元数据的网络传输。
 * 在任务复用JVM的场景下，用于标识当前运行任务的JVM进程信息。
 */
class JvmContext implements Writable {

  /** 日志记录器，用于记录JVM上下文相关的日志信息 */
  public static final Logger LOG =
      LoggerFactory.getLogger(JvmContext.class);
  
  /** JVM全局唯一标识，包含作业ID和JVM编号等信息 */
  JVMId jvmId;
  /** JVM对应的操作系统进程ID */
  String pid;
  
  /**
   * 默认构造函数，初始化空的JVM上下文对象
   */
  JvmContext() {
    jvmId = new JVMId();
    pid = "";
  }
  
  /**
   * 构造函数，使用指定的JVM标识和进程ID创建上下文对象
   * @param id JVM全局唯一标识
   * @param pid 操作系统进程ID
   */
  JvmContext(JVMId id, String pid) {
    jvmId = id;
    this.pid = pid;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // 反序列化JVM标识
    jvmId.readFields(in);
    // 反序列化进程ID字符串
    this.pid = Text.readString(in);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    // 序列化JVM标识
    jvmId.write(out);
    // 序列化进程ID字符串
    Text.writeString(out, pid);
  }
}