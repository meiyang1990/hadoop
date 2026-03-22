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
 * Map Task完成事件更新消息，用于TaskTracker与子Reduce任务之间的通信，
 * 携带已完成Map任务的事件列表，同时指示Reduce任务是否需要重置事件获取索引。
 * 在MapReduce shuffle阶段，Reduce任务需要拉取已完成Map任务的输出，该类封装
 * TaskTracker下发给Reduce任务的事件更新信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapTaskCompletionEventsUpdate implements Writable {
  TaskCompletionEvent[] events;
  boolean reset;

  /**
   * 空构造函数，用于反序列化
   */
  public MapTaskCompletionEventsUpdate() { }

  /**
   * 构造Map Task完成事件更新对象
   * @param events Map任务完成事件数组
   * @param reset 是否要求Reduce任务重置事件索引
   */
  public MapTaskCompletionEventsUpdate(TaskCompletionEvent[] events,
      boolean reset) {
    this.events = events;
    this.reset = reset;
  }

  /**
   * 获取是否需要重置事件索引的标识
   * @return true表示需要重置，false表示不需要
   */
  public boolean shouldReset() {
    return reset;
  }

  /**
   * 获取本次更新携带的Map任务完成事件列表
   * @return Map任务完成事件数组
   */
  public TaskCompletionEvent[] getMapTaskCompletionEvents() {
    return events;
  }

  /**
   * 将对象序列化输出到指定DataOutput
   * @param out 输出流
   * @throws IOException 输出异常
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // 写入重置标识
    out.writeBoolean(reset);
    // 写入事件数量
    out.writeInt(events.length);
    // 逐个序列化每个完成事件
    for (TaskCompletionEvent event : events) {
      event.write(out);
    }
  }

  /**
   * 从指定DataInput反序列化对象
   * @param in 输入流
   * @throws IOException 输入异常
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取重置标识
    reset = in.readBoolean();
    // 读取事件数量，初始化事件数组
    events = new TaskCompletionEvent[in.readInt()];
    // 逐个反序列化每个完成事件
    for (int i = 0; i < events.length; ++i) {
      events[i] = new TaskCompletionEvent();
      events[i].readFields(in);
    }
  }
}