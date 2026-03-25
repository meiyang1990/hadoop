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

package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

/**
 * MapReduce各类标识符的抽象基类，内部使用整数存储ID值，是JobID、TaskID、TaskAttemptID的父类。
 * 为所有MapReduce核心实体提供统一的ID表示、序列化和比较能力。
 * 
 * @see JobID
 * @see TaskID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ID implements WritableComparable<ID> {
  // ID字符串表示中的分隔符
  protected static final char SEPARATOR = '_';
  // 存储ID的整数值
  protected int id;

  /**
   * 根据给定整数构造ID对象
   * @param id 标识符的整数值
   */
  public ID(int id) {
    this.id = id;
  }

  protected ID() {
  }

  /**
   * 获取ID对应的整数值
   * @return 标识符的整数表示
   */
  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return String.valueOf(id);
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if(o == null)
      return false;
    if (o.getClass() == this.getClass()) {
      ID that = (ID) o;
      return this.id == that.id;
    }
    else
      return false;
  }

  /**
   * 按照ID的整数值比较两个ID对象的大小
   * @param that 待比较的另一个ID对象
   * @return 比较结果：当前ID小于、等于、大于目标ID时分别返回负数、0、正数
   */
  public int compareTo(ID that) {
    return this.id - that.id;
  }

  /**
   * 从输入流反序列化读取ID值
   * @param in 数据输入流
   * @throws IOException 输入流读取异常
   */
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
  }

  /**
   * 将ID值序列化写入输出流
   * @param out 数据输出流
   * @throws IOException 输出流写入异常
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
  }
  
}