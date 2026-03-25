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
 * TaskID 表示Map或Reduce任务的不可变唯一标识符。每个TaskID对应多次任务执行尝试，
 * 每次尝试由TaskAttemptID唯一标识。
 * 
 * TaskID由三部分组成：第一部分是所属作业的{@link JobID}，第二部分是'm'或'r'表示任务类型（Map/Reduce），
 * 第三部分是任务编号。<br>
 * 示例TaskID：<code>task_200707121733_0003_m_000005</code>，表示jobtracker启动时间为
 * <code>200707121733</code>的第三个作业中的第五个Map任务。
 * <p>
 * 应用程序不应自行构造或解析TaskID字符串，应使用合适的构造函数或{@link #forName(String)}方法。
 * 
 * @see JobID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskID extends org.apache.hadoop.mapreduce.TaskID {

  /**
   * 根据给定的JobID构造TaskID对象
   * @param jobId 该任务所属的JobID
   * @param isMap 是否为Map任务
   * @param id 任务编号
   * @deprecated 使用{@link #TaskID(String, int, TaskType, int)}
   */
  @Deprecated
  public TaskID(org.apache.hadoop.mapreduce.JobID jobId, boolean isMap,int id) {
    this(jobId, isMap ? TaskType.MAP : TaskType.REDUCE, id);
  }
   
  /**
   * 根据给定参数构造TaskID对象
   * @param jtIdentifier jobTracker标识符
   * @param jobId 作业编号
   * @param isMap 是否为Map任务
   * @param id 任务编号
   * @deprecated 使用{@link #TaskID(org.apache.hadoop.mapreduce.JobID, TaskType, int)}
   */
  @Deprecated
  public TaskID(String jtIdentifier, int jobId, boolean isMap, int id) {
    this(jtIdentifier, jobId, isMap ? TaskType.MAP : TaskType.REDUCE, id);
  }
    
  /**
   * 根据给定的JobID构造TaskID对象
   * @param jobId 该任务所属的JobID
   * @param type 任务类型（Map/Reduce等）
   * @param id 任务编号
   */
  public TaskID(org.apache.hadoop.mapreduce.JobID jobId, TaskType type,int id) {
    super(jobId, type, id);
  }
  
  /**
   * 根据给定参数构造TaskID对象
   * @param jtIdentifier jobTracker标识符
   * @param jobId 作业编号
   * @param type 任务类型
   * @param id 任务编号
   */
  public TaskID(String jtIdentifier, int jobId, TaskType type, int id) {
    this(new JobID(jtIdentifier, jobId), type, id);
  }
  
  /**
   * 无参构造函数，创建空TaskID对象
   */
  public TaskID() {
    super(new JobID(), TaskType.REDUCE, 0);
  }
  
  /**
   * 将新版本org.apache.hadoop.mapreduce.TaskID降级转换为旧版mapred.TaskID，用于兼容旧API
   * @param old 新版本或旧版本的TaskID对象
   * @return 转换后的旧版TaskID对象
   */
  public static TaskID downgrade(org.apache.hadoop.mapreduce.TaskID old) {
    if (old instanceof TaskID) {
      return (TaskID) old;
    } else {
      return new TaskID(JobID.downgrade(old.getJobID()), old.getTaskType(), 
                        old.getId());
    }
  }

  /**
   * 从DataInput中读取并构造TaskID对象
   * @param in 数据输入流
   * @return 读取得到的TaskID对象
   * @throws IOException 读取过程中发生IO异常
   * @deprecated
   */
  @Deprecated
  public static TaskID read(DataInput in) throws IOException {
    TaskID tipId = new TaskID();
    tipId.readFields(in);
    return tipId;
  }
  
  /**
   * 获取该任务所属的旧版JobID
   * @return 所属的旧版JobID
   */
  public JobID getJobID() {
    return (JobID) super.getJobID();
  }

  /** 
   * 获取匹配TaskID的正则表达式，参数传入null表示对应部分匹配任意值
   * @param jtIdentifier jobTracker标识符，null表示任意
   * @param jobId 作业编号，null表示任意
   * @param isMap 是否为Map任务，null表示任意
   * @param taskId 任务编号，null表示任意
   * @return 匹配TaskID的正则表达式字符串
   * @deprecated 使用{@link TaskID#getTaskIDsPattern(String, Integer, TaskType, Integer)}
   */
  @Deprecated
  public static String getTaskIDsPattern(String jtIdentifier, Integer jobId
      , Boolean isMap, Integer taskId) {
    return getTaskIDsPattern(jtIdentifier, jobId,
	isMap ? TaskType.MAP : TaskType.REDUCE, taskId);
  }
  
  /** 
   * 获取匹配TaskID的正则表达式，参数传入null表示对应部分匹配任意值
   * @param jtIdentifier jobTracker标识符，null表示任意
   * @param jobId 作业编号，null表示任意
   * @param type 任务类型，null表示任意
   * @param taskId 任务编号，null表示任意
   * @return 匹配TaskID的正则表达式字符串
   */
  @Deprecated
  public static String getTaskIDsPattern(String jtIdentifier, Integer jobId
      , TaskType type, Integer taskId) {
    StringBuilder builder = new StringBuilder(TASK).append(SEPARATOR)
      .append(getTaskIDsPatternWOPrefix(jtIdentifier, jobId, type, taskId));
    return builder.toString();
  }
  
  /**
   * 获取不带前缀task_部分的TaskID正则表达式
   * @param jtIdentifier jobTracker标识符，null表示任意
   * @param jobId 作业编号，null表示任意
   * @param type 任务类型，null表示任意
   * @param taskId 任务编号，null表示任意
   * @return 拼接好的正则表达式StringBuilder
   * @deprecated
   */
  @Deprecated
  static StringBuilder getTaskIDsPatternWOPrefix(String jtIdentifier
      , Integer jobId, TaskType type, Integer taskId) {
    StringBuilder builder = new StringBuilder();
    builder.append(JobID.getJobIDsPatternWOPrefix(jtIdentifier, jobId))
      .append(SEPARATOR)
      .append(type != null ? 
          (org.apache.hadoop.mapreduce.TaskID.getRepresentingCharacter(type)) : 
            org.apache.hadoop.mapreduce.TaskID.getAllTaskTypes()).
            append(SEPARATOR)
      .append(taskId != null ? idFormat.format(taskId) : "[0-9]*");
    return builder;
  }

  /**
   * 从TaskID字符串解析生成TaskID对象
   * @param str TaskID字符串
   * @return 解析得到的TaskID对象
   * @throws IllegalArgumentException 字符串格式不正确时抛出
   */
  public static TaskID forName(String str
                               ) throws IllegalArgumentException {
    return (TaskID) org.apache.hadoop.mapreduce.TaskID.forName(str);
  }

}