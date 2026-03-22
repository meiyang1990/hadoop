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

/**
 * TaskAttemptID represents the immutable and unique identifier for 
 * a task attempt. Each task attempt is one particular instance of a Map or
 * Reduce Task identified by its TaskID. 
 * 
 * TaskAttemptID consists of 2 parts. First part is the 
 * {@link TaskID}, that this TaskAttemptID belongs to.
 * Second part is the task attempt number. <br> 
 * An example TaskAttemptID is : 
 * <code>attempt_200707121733_0003_m_000005_0</code> , which represents the
 * zeroth task attempt for the fifth map task in the third job 
 * running at the jobtracker started at <code>200707121733</code>.
 * <p>
 * Applications should never construct or parse TaskAttemptID strings
 * , but rather use appropriate constructors or {@link #forName(String)} 
 * method. 
 * 
 * @see JobID
 * @see TaskID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * MapReduce任务尝试的唯一标识符类，用于标识同一个Task的不同运行尝试
 * 当Task运行失败时，会启动新的尝试，每个尝试都有独立的ID
 */
public class TaskAttemptID extends org.apache.hadoop.mapred.ID {
  protected static final String ATTEMPT = "attempt";
  private TaskID taskId;
  
  /**
   * Constructs a TaskAttemptID object from given {@link TaskID}.  
   * @param taskId TaskID that this task belongs to  
   * @param id the task attempt number
   * 根据所属TaskID和尝试编号构造任务尝试ID
   */
  public TaskAttemptID(TaskID taskId, int id) {
    super(id);
    if(taskId == null) {
      throw new IllegalArgumentException("taskId cannot be null");
    }
    this.taskId = taskId;
  }
  
  /**
   * Constructs a TaskId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param type the TaskType 
   * @param taskId taskId number
   * @param id the task attempt number
   * 根据JobTracker标识、作业编号、任务类型、任务编号、尝试编号构造任务尝试ID
   */
  public TaskAttemptID(String jtIdentifier, int jobId, TaskType type, 
                       int taskId, int id) {
    this(new TaskID(jtIdentifier, jobId, type, taskId), id);
  }

  /**
   * Constructs a TaskId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number
   * @param isMap whether the tip is a map
   * @param taskId taskId number
   * @param id the task attempt number
   * 根据JobTracker标识、作业编号、是否为Map任务、任务编号、尝试编号构造任务尝试ID（已废弃）
   */
  @Deprecated
  public TaskAttemptID(String jtIdentifier, int jobId, boolean isMap,
                       int taskId, int id) {
    this(new TaskID(jtIdentifier, jobId, isMap, taskId), id);
  }
  
  /**
   * 空构造方法，用于反序列化
   */
  public TaskAttemptID() { 
    taskId = new TaskID();
  }
  
  /**
   * 获取当前任务尝试所属作业的ID
   * @return 当前任务尝试所属的JobID
   */
  public JobID getJobID() {
    return taskId.getJobID();
  }
  
  /**
   * 获取当前任务尝试所属任务的ID
   * @return 当前任务尝试所属的TaskID
   */
  public TaskID getTaskID() {
    return taskId;
  }
  
  /**
   * 判断当前任务是否为Map任务（已废弃）
   * @return 如果是Map任务返回true，否则返回false
   */
  @Deprecated
  public boolean isMap() {
    return taskId.isMap();
  }
    
  /**
   * 获取当前任务的类型（Map/Reduce）
   * @return 当前任务的TaskType枚举
   */
  public TaskType getTaskType() {
    return taskId.getTaskType();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskAttemptID that = (TaskAttemptID)o;
    return this.taskId.equals(that.taskId);
  }
  
  /**
   * Add the unique string to the StringBuilder
   * @param builder the builder to append ot
   * @return the builder that was passed in.
   * 将当前任务尝试ID的唯一字符串追加到StringBuilder
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return taskId.appendTo(builder).append(SEPARATOR).append(id);
  }
  
  @Override
  /**
   * 从输入流反序列化TaskAttemptID对象
   */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    taskId.readFields(in);
  }

  @Override
  /**
   * 将TaskAttemptID对象序列化到输出流
   */
  public void write(DataOutput out) throws IOException {
    super.write(out);
    taskId.write(out);
  }

  @Override
  /**
   * 计算TaskAttemptID的哈希值
   */
  public int hashCode() {
    return taskId.hashCode() * 5 + id;
  }
  
  /**Compare TaskIds by first tipIds, then by task numbers. */
  @Override
  /**
   * 按TaskID比较后再按尝试编号比较，实现TaskAttemptID的排序
   */
  public int compareTo(ID o) {
    TaskAttemptID that = (TaskAttemptID)o;
    int tipComp = this.taskId.compareTo(that.taskId);
    if(tipComp == 0) {
      return this.id - that.id;
    }
    else return tipComp;
  }
  @Override
  /**
   * 转换为标准格式的字符串表示
   */
  public String toString() { 
    return appendTo(new StringBuilder(ATTEMPT)).toString();
  }

  /** Construct a TaskAttemptID object from given string 
   * @return constructed TaskAttemptID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   * 从字符串格式解析构造TaskAttemptID对象
   */
  public static TaskAttemptID forName(String str
                                      ) throws IllegalArgumentException {
    if(str == null)
      return null;
    String exceptionMsg = null;
    try {
      // 按分隔符拆分字符串各部分
      String[] parts = str.split(Character.toString(SEPARATOR));
      // 验证格式长度正确
      if(parts.length == 6) {
        // 验证开头标识正确
        if(parts[0].equals(ATTEMPT)) {
          // 解析任务类型
          String type = parts[3];
          TaskType t = TaskID.getTaskType(type.charAt(0));
          if(t != null) {
            // 构造返回TaskAttemptID对象（兼容旧版mapred包类型）
            return new org.apache.hadoop.mapred.TaskAttemptID
            (parts[1],
             Integer.parseInt(parts[2]),
             t, Integer.parseInt(parts[4]), 
             Integer.parseInt(parts[5]));  
          } else
            exceptionMsg = "Bad TaskType identifier. TaskAttemptId string : "
                + str + " is not properly formed.";
        }
      }
    } catch (Exception ex) {
      // 解析异常，统一处理
    }
    if (exceptionMsg == null) {
      exceptionMsg = "TaskAttemptId string : " + str
          + " is not properly formed";
    }
    throw new IllegalArgumentException(exceptionMsg);
  }

}