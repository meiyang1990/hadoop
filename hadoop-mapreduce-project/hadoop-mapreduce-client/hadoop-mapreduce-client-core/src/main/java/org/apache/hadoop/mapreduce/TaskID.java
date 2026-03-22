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
import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableUtils;


/**
 * TaskID 表示 MapReduce 中一个Map或Reduce任务的不可变唯一标识符。
 * 每个任务可以有多次执行尝试，每次尝试由 TaskAttemptID 唯一标识。
 * 
 * TaskID由三部分组成：第一部分是所属作业的{@link JobID}；第二部分是'm'或'r'（现在扩展支持更多类型）标识任务类型；
 * 第三部分是任务编号。<br> 
 * 示例 TaskID：<code>task_200707121733_0003_m_000005</code>，表示作业跟踪器
 * 启动于 200707121733 的第三个作业中的第五个 Map 任务。
 * <p>
 * 应用程序不应自行构造或解析TaskID字符串，应使用合适的构造方法或{@link #forName(String)}方法。
 * 
 * @see JobID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskID extends org.apache.hadoop.mapred.ID {
  protected static final String TASK = "task";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  public static final String TASK_ID_REGEX = TASK + "_(\\d+)_(\\d+)_" +
      CharTaskTypeMaps.allTaskTypes + "_(\\d+)";
  public static final Pattern taskIdPattern = Pattern.compile(TASK_ID_REGEX);

  static {
    // 关闭数字分组，不显示千分位分隔符
    idFormat.setGroupingUsed(false);
    // 设置最小数字位数，不足补零
    idFormat.setMinimumIntegerDigits(6);
  }
  
  private JobID jobId;
  private TaskType type;
  
  /**
   * 根据给定的JobID构造TaskID对象。
   *
   * @param jobId 该任务所属作业的JobID
   * @param type 任务的{@link TaskType}类型
   * @param id 任务编号
   */
  public TaskID(JobID jobId, TaskType type, int id) {
    super(id);
    if(jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
    this.type = type;
  }
  
  /**
   * 根据给定的组件参数构造TaskID对象。
   *
   * @param jtIdentifier jobTracker标识符
   * @param jobId 作业编号
   * @param type 任务类型
   * @param id 任务编号
   */
  public TaskID(String jtIdentifier, int jobId, TaskType type, int id) {
    this(new JobID(jtIdentifier, jobId), type, id);
  }

  /**
   * 根据给定的JobID构造TaskID对象，已弃用。
   *
   * @param jobId 该任务所属作业的JobID
   * @param isMap 是否为Map任务
   * @param id 任务编号
   */
  @Deprecated
  public TaskID(JobID jobId, boolean isMap, int id) {
    this(jobId, isMap ? TaskType.MAP : TaskType.REDUCE, id);
  }

  /**
   * 根据给定的组件参数构造TaskID对象，已弃用。
   *
   * @param jtIdentifier jobTracker标识符
   * @param jobId 作业编号
   * @param isMap 是否为Map任务
   * @param id 任务编号
   */
  @Deprecated
  public TaskID(String jtIdentifier, int jobId, boolean isMap, int id) {
    this(new JobID(jtIdentifier, jobId), isMap, id);
  }
  
  /**
   * 供Writable反序列化使用的默认构造方法。
   * 将任务类型设置为{@link TaskType#REDUCE}，ID设置为0，作业ID设置为空JobID。
   */
  public TaskID() { 
    this(new JobID(), TaskType.REDUCE, 0);
  }
  
  /**
   * 获取该任务所属作业的JobID。
   *
   * @return 所属作业的JobID对象
   */
  public JobID getJobID() {
    return jobId;
  }
  
  /**
   * 判断该TaskID是否为Map任务ID，已弃用。
   *
   * @return 如果是Map任务返回true，否则返回false
   */
  @Deprecated
  public boolean isMap() {
    return type == TaskType.MAP;
  }
    
  /**
   * 获取任务类型。
   *
   * @return 任务的TaskType枚举值
   */
  public TaskType getTaskType() {
    return type;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskID that = (TaskID)o;
    // 比较作业ID和任务类型是否都相同
    return this.type == that.type && this.jobId.equals(that.jobId);
  }

  /**
   * 比较两个TaskID的大小，首先比较作业ID，然后比较任务编号。
   * Reducer任务定义为比Mapper任务大。
   *
   * @param o 要比较的目标TaskID
   * @return 0表示相等，正数表示当前TaskID更大，负数表示当前TaskID更小
   */
  @Override
  public int compareTo(ID o) {
    TaskID that = (TaskID)o;
    // 先比较所属作业ID
    int jobComp = this.jobId.compareTo(that.jobId);
    if(jobComp == 0) {
      // 作业ID相同，比较任务类型
      if(this.type == that.type) {
        // 类型相同，比较任务编号
        return this.id - that.id;
      }
      else {
        // 类型不同，按枚举顺序比较
        return this.type.compareTo(that.type);
      }
    }
    else return jobComp;
  }
  @Override
  public String toString() { 
    // 拼接生成TaskID字符串
    return appendTo(new StringBuilder(TASK)).toString();
  }

  /**
   * 将TaskID的唯一标识字符串追加到给定的StringBuilder。
   *
   * @param builder 要追加内容的StringBuilder
   * @return 传入的StringBuilder对象
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    // 依次拼接作业ID、分隔符、任务类型字符、分隔符、格式化任务编号
    return jobId.appendTo(builder).
                 append(SEPARATOR).
                 append(CharTaskTypeMaps.getRepresentingCharacter(type)).
                 append(SEPARATOR).
                 append(idFormat.format(id));
  }
  
  @Override
  public int hashCode() {
    // 基于作业ID和任务编号计算哈希值
    return jobId.hashCode() * 524287 + id;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取父类字段
    super.readFields(in);
    // 读取作业ID
    jobId.readFields(in);
    // 读取任务类型枚举
    type = WritableUtils.readEnum(in, TaskType.class);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // 写入父类字段
    super.write(out);
    // 写入作业ID
    jobId.write(out);
    // 写入任务类型枚举
    WritableUtils.writeEnum(out, type);
  }
  
  /**
   * 从给定字符串解析构造TaskID对象。
   *
   * @param str 要解析的TaskID字符串
   * @return 构造完成的TaskID对象，如果输入字符串为null返回null
   * @throws IllegalArgumentException 如果输入字符串格式不正确
   */
  public static TaskID forName(String str) 
    throws IllegalArgumentException {
    if(str == null)
      return null;
    // 使用正则匹配TaskID格式
    Matcher m = taskIdPattern.matcher(str);
    if (m.matches()) {
      // 解析各个分组并构造TaskID
      return new org.apache.hadoop.mapred.TaskID(m.group(1),
          Integer.parseInt(m.group(2)),
          CharTaskTypeMaps.getTaskType(m.group(3).charAt(0)),
          Integer.parseInt(m.group(4)));
    }
    String exceptionMsg = "TaskId string : " + str + " is not properly formed" +
        "\nReason: " + m.toString();
    throw new IllegalArgumentException(exceptionMsg);
  }
  /**
   * 获取任务类型对应的表示字符。
   *
   * @param type 任务类型
   * @return 表示该任务类型的字符
   */
  public static char getRepresentingCharacter(TaskType type) {
    return CharTaskTypeMaps.getRepresentingCharacter(type);
  }
  /**
   * 获取字符对应的任务类型。
   *
   * @param c 表示任务类型的字符
   * @return 对应的TaskType枚举值
   */
  public static TaskType getTaskType(char c) {
    return CharTaskTypeMaps.getTaskType(c);
  }
  
  /**
   * 获取所有可能任务类型字符的正则表达式字符串。
   *
   * @return 所有任务类型字符组成的字符串
   */
  public static String getAllTaskTypes() {
    return CharTaskTypeMaps.allTaskTypes;
  }

  /**
   * 维护任务类型枚举和其字符表示之间的双向映射关系
   */
  static class CharTaskTypeMaps {
    // 任务类型到字符的映射
    private static EnumMap<TaskType, Character> typeToCharMap = 
      new EnumMap<TaskType,Character>(TaskType.class);
    // 字符到任务类型的映射
    private static Map<Character, TaskType> charToTypeMap = 
      new HashMap<Character, TaskType>();
    // 所有可能任务类型字符的正则表达式
    static String allTaskTypes = "(m|r|s|c|t)";
    static {
      // 初始化双向映射
      setupTaskTypeToCharMapping();
      setupCharToTaskTypeMapping();
    }
    
    /**
     * 初始化任务类型到字符的映射
     */
    private static void setupTaskTypeToCharMapping() {
      typeToCharMap.put(TaskType.MAP, 'm');
      typeToCharMap.put(TaskType.REDUCE, 'r');
      typeToCharMap.put(TaskType.JOB_SETUP, 's');
      typeToCharMap.put(TaskType.JOB_CLEANUP, 'c');
      typeToCharMap.put(TaskType.TASK_CLEANUP, 't');
    }

    /**
     * 初始化字符到任务类型的映射
     */
    private static void setupCharToTaskTypeMapping() {
      charToTypeMap.put('m', TaskType.MAP);
      charToTypeMap.put('r', TaskType.REDUCE);
      charToTypeMap.put('s', TaskType.JOB_SETUP);
      charToTypeMap.put('c', TaskType.JOB_CLEANUP);
      charToTypeMap.put('t', TaskType.TASK_CLEANUP);
    }

    static char getRepresentingCharacter(TaskType type) {
      return typeToCharMap.get(type);
    }
    static TaskType getTaskType(char c) {
      return charToTypeMap.get(c);
    }
  }

}