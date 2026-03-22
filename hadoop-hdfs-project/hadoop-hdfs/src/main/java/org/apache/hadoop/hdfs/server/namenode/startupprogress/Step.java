// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：NameNode启动阶段的单个启动步骤，记录启动过程中每个子任务的信息
 * 表示NameNode在某个启动阶段中执行的一个具体步骤，用于跟踪启动进度
 * A step performed by the namenode during a {@link Phase} of startup.
 */
@InterfaceAudience.Private
public class Step implements Comparable<Step> {
  // 全局自增序列号生成器，保证每个Step生成唯一的顺序编号
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private final String file;
  private final int sequenceNumber;
  private final long size;
  private final StepType type;

  /**
   * 仅指定步骤类型的构造方法，创建一个不关联文件、未指定大小的启动步骤
   * 
   * @param type 步骤类型
   */
  public Step(StepType type) {
    this(type, null, Long.MIN_VALUE);
  }

  /**
   * 仅指定处理文件的构造方法，创建一个未指定类型和大小的启动步骤
   * 
   * @param file 处理的文件路径
   */
  public Step(String file) {
    this(null, file, Long.MIN_VALUE);
  }

  /**
   * 指定处理文件和文件大小的构造方法，创建一个未指定类型的启动步骤
   * 
   * @param file 处理的文件路径
   * @param size 文件大小（字节）
   */
  public Step(String file, long size) {
    this(null, file, size);
  }

  /**
   * 指定步骤类型和处理文件的构造方法，创建一个未指定大小的启动步骤
   * 
   * @param type 步骤类型
   * @param file 处理的文件路径
   */
  public Step(StepType type, String file) {
    this(type, file, Long.MIN_VALUE);
  }

  /**
   * 完整参数构造方法，创建一个指定所有属性的启动步骤，自动分配全局唯一序列号
   * 
   * @param type 步骤类型
   * @param file 处理的文件路径
   * @param size 文件大小（字节）
   */
  public Step(StepType type, String file, long size) {
    this.file = file;
    this.sequenceNumber = SEQUENCE.incrementAndGet();
    this.size = size;
    this.type = type;
  }

  /**
   * 比较两个Step对象，用于排序：先按文件路径排序，同文件内按创建顺序排序
   * 由于JDK并发默认不保留插入顺序，通过序列号保证读取时可恢复插入顺序
   * @param other 待比较的另一个Step对象
   * @return 比较结果：负数表示当前对象更小，0表示相等，正数表示当前对象更大
   */
  @Override
  public int compareTo(Step other) {
    // Sort steps by file and then sequentially within the file to achieve the
    // desired order.  There is no concurrent map structure in the JDK that
    // maintains insertion order, so instead we attach a sequence number to each
    // step and sort on read.
    return new CompareToBuilder().append(file, other.file)
      .append(sequenceNumber, other.sequenceNumber).toComparison();
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null || otherObj.getClass() != getClass()) {
      return false;
    }
    Step other = (Step)otherObj;
    return new EqualsBuilder().append(this.file, other.file)
      .append(this.size, other.size).append(this.type, other.type).isEquals();
  }

  /**
   * 获取当前步骤关联的文件名称
   * 
   * @return 关联的文件名称，无关联文件则返回null
   */
  public String getFile() {
    return file;
  }

  /**
   * 获取当前步骤处理文件的大小
   * 
   * @return 文件大小（字节），未指定大小则返回Long.MIN_VALUE
   */
  public long getSize() {
    return size;
  }

  /**
   * 获取当前步骤的类型
   * 
   * @return 步骤类型，未指定类型则返回null
   */
  public StepType getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(file).append(size).append(type)
      .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("file", file)
        .append("sequenceNumber", sequenceNumber)
        .append("size", size)
        .append("type", type)
        .toString();
  }
}