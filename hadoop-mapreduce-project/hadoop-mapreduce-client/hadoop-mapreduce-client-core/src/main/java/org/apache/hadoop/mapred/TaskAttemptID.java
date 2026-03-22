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
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskType;

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
 *
 * 旧版MapReduce API的任务尝试ID类，封装单个任务某次运行尝试的唯一标识
 * 每个任务可能因推测执行或失败重试产生多个运行尝试，每个尝试对应一个唯一ID
 * 继承自新版org.apache.hadoop.mapreduce.TaskAttemptID，兼容旧API接口
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskAttemptID extends org.apache.hadoop.mapreduce.TaskAttemptID {
  
  /**
   * Constructs a TaskAttemptID object from given {@link TaskID}.  
   * @param taskId TaskID that this task belongs to  
   * @param id the task attempt number
   * 根据所属任务ID和尝试编号构造任务尝试ID
   */
  public TaskAttemptID(TaskID taskId, int id) {
    super(taskId, id);
  }
  
  /**
   * Constructs a TaskId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param isMap whether the tip is a map 
   * @param taskId taskId number
   * @param id the task attempt number
   * @deprecated Use {@link #TaskAttemptID(String, int, TaskType, int, int)}.
   * 根据JobTracker标识、作业编号、任务类型、任务编号、尝试编号构造任务尝试ID（已废弃）
   */
  @Deprecated
  public TaskAttemptID(String jtIdentifier, int jobId, boolean isMap, 
      int taskId, int id) {
    this(jtIdentifier, jobId, isMap ? TaskType.MAP : TaskType.REDUCE, taskId,
	id);
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
   * 无参构造函数，创建空的任务尝试ID对象，用于反序列化
   */
  public TaskAttemptID() { 
    super(new TaskID(), 0);
  }

  /**
   * Downgrade a new TaskAttemptID to an old one
   * @param old the new id
   * @return either old or a new TaskAttemptID constructed to match old
   * 将新版API的TaskAttemptID转换为旧版API的TaskAttemptID，兼容旧API调用
   */
  public static 
  TaskAttemptID downgrade(org.apache.hadoop.mapreduce.TaskAttemptID old) {
    if (old instanceof TaskAttemptID) {
      return (TaskAttemptID) old;
    } else {
      return new TaskAttemptID(TaskID.downgrade(old.getTaskID()), old.getId());
    }
  }

  /**
   * 获取当前任务尝试所属任务的旧版TaskID
   * @return 旧版TaskID对象
   */
  public TaskID getTaskID() {
    return (TaskID) super.getTaskID();
  }

  /**
   * 获取当前任务尝试所属作业的旧版JobID
   * @return 旧版JobID对象
   */
  public JobID getJobID() {
    return (JobID) super.getJobID();
  }

  /**
   * 从DataInput流中反序列化读取TaskAttemptID（已废弃）
   * @param in 输入数据流
   * @return 反序列化得到的TaskAttemptID对象
   * @throws IOException 读取数据时抛出IO异常
   */
  @Deprecated
  public static TaskAttemptID read(DataInput in) throws IOException {
    TaskAttemptID taskId = new TaskAttemptID();
    taskId.readFields(in);
    return taskId;
  }
  
  /** Construct a TaskAttemptID object from given string 
   * @return constructed TaskAttemptID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   * 从字符串格式解析构造TaskAttemptID对象
   */
  public static TaskAttemptID forName(String str
                                      ) throws IllegalArgumentException {
    return (TaskAttemptID) 
             org.apache.hadoop.mapreduce.TaskAttemptID.forName(str);
  }
  
  /** 
   * Returns a regex pattern which matches task attempt IDs. Arguments can 
   * be given null, in which case that part of the regex will be generic.  
   * For example to obtain a regex matching <i>all task attempt IDs</i> 
   * of <i>any jobtracker</i>, in <i>any job</i>, of the <i>first 
   * map task</i>, we would use :
   * <pre> 
   * TaskAttemptID.getTaskAttemptIDsPattern(null, null, true, 1, null);
   * </pre>
   * which will return :
   * <pre> "attempt_[^_]*_[0-9]*_m_000001_[0-9]*" </pre> 
   * @param jtIdentifier jobTracker identifier, or null
   * @param jobId job number, or null
   * @param isMap whether the tip is a map, or null 
   * @param taskId taskId number, or null
   * @param attemptId the task attempt number, or null
   * @return a regex pattern matching TaskAttemptIDs
   * 生成匹配任务尝试ID的正则表达式（已废弃，基于布尔类型表示任务类型）
   */
  @Deprecated
  public static String getTaskAttemptIDsPattern(String jtIdentifier,
      Integer jobId, Boolean isMap, Integer taskId, Integer attemptId) {
    return getTaskAttemptIDsPattern(jtIdentifier, jobId,
	isMap ? TaskType.MAP : TaskType.REDUCE, taskId, attemptId);
  }
  
  /** 
   * Returns a regex pattern which matches task attempt IDs. Arguments can 
   * be given null, in which case that part of the regex will be generic.  
   * For example to obtain a regex matching <i>all task attempt IDs</i> 
   * of <i>any jobtracker</i>, in <i>any job</i>, of the <i>first 
   * map task</i>, we would use :
   * <pre> 
   * TaskAttemptID.getTaskAttemptIDsPattern(null, null, TaskType.MAP, 1, null);
   * </pre>
   * which will return :
   * <pre> "attempt_[^_]*_[0-9]*_m_000001_[0-9]*" </pre> 
   * @param jtIdentifier jobTracker identifier, or null
   * @param jobId job number, or null
   * @param type the {@link TaskType} 
   * @param taskId taskId number, or null
   * @param attemptId the task attempt number, or null
   * @return a regex pattern matching TaskAttemptIDs
   * 生成匹配任务尝试ID的正则表达式（已废弃，基于TaskType表示任务类型）
   */
  @Deprecated
  public static String getTaskAttemptIDsPattern(String jtIdentifier,
      Integer jobId, TaskType type, Integer taskId, Integer attemptId) {
    // 构建正则前缀"attempt_"
    StringBuilder builder = new StringBuilder(ATTEMPT).append(SEPARATOR);
    // 添加不带前缀的任务尝试匹配部分
    builder.append(getTaskAttemptIDsPatternWOPrefix(jtIdentifier, jobId,
        type, taskId, attemptId));
    return builder.toString();
  }
  
  /**
   * 生成不带前缀"attempt_"的任务尝试ID正则匹配部分（已废弃）
   * @param jtIdentifier jobTracker identifier, or null
   * @param jobId job number, or null
   * @param type the {@link TaskType} 
   * @param taskId taskId number, or null
   * @param attemptId the task attempt number, or null
   * @return 正则匹配部分的StringBuilder
   */
  @Deprecated
  static StringBuilder getTaskAttemptIDsPatternWOPrefix(String jtIdentifier
      , Integer jobId, TaskType type, Integer taskId, Integer attemptId) {
    StringBuilder builder = new StringBuilder();
    // 拼接任务ID部分的正则
    builder.append(TaskID.getTaskIDsPatternWOPrefix(jtIdentifier
        , jobId, type, taskId))
        // 添加分隔符
        .append(SEPARATOR)
        // 添加尝试编号匹配规则：指定则匹配固定值，否则匹配任意数字
        .append(attemptId != null ? attemptId : "[0-9]*");
    return builder;
  }
}