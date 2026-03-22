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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * JVM 任务封装类，用于在进程间序列化传输任务信息，实现 Hadoop Writable 序列化接口。
 * 负责封装 MapReduce 任务以及JVM是否需要退出的指令，供 child JVM 与 TaskTracker 通信使用。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JvmTask implements Writable {
  Task t;
  boolean shouldDie;

  /**
   * 构造 JvmTask 对象，封装任务实例和退出指令。
   * @param t 待执行的 MapReduce 任务实例
   * @param shouldDie 指示当前JVM执行完任务后是否需要退出
   */
  public JvmTask(Task t, boolean shouldDie) {
    this.t = t;
    this.shouldDie = shouldDie;
  }

  /**
   * 空构造函数，用于反序列化时创建对象。
   */
  public JvmTask() {}

  /**
   * 获取封装的任务实例。
   * @return 待执行的 MapReduce 任务
   */
  public Task getTask() {
    return t;
  }

  /**
   * 获取当前JVM是否应该退出的指令。
   * @return true 表示JVM执行完任务后需要退出，false表示继续复用JVM
   */
  public boolean shouldDie() {
    return shouldDie;
  }

  /**
   * 将 JvmTask 对象序列化输出到数据流。
   * @param out 输出数据流
   * @throws IOException 序列化过程中IO异常
   */
  public void write(DataOutput out) throws IOException {
    // 写入JVM退出标识
    out.writeBoolean(shouldDie);
    // 检查是否存在任务对象
    if (t != null) {
      out.writeBoolean(true);
      // 写入任务类型标识：Map任务还是Reduce任务
      out.writeBoolean(t.isMapTask());
      // 序列化任务对象
      t.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  /**
   * 从输入数据流反序列化 JvmTask 对象。
   * @param in 输入数据流
   * @throws IOException 反序列化过程中IO异常
   */
  public void readFields(DataInput in) throws IOException {
    // 读取JVM退出标识
    shouldDie = in.readBoolean();
    // 读取是否存在任务的标识
    boolean taskComing = in.readBoolean();
    if (taskComing) {
      // 读取任务类型
      boolean isMap = in.readBoolean();
      // 根据任务类型创建对应实例
      if (isMap) {
        t = new MapTask();
      } else {
        t = new ReduceTask();
      }
      // 反序列化任务对象字段
      t.readFields(in);
    }
  }
}