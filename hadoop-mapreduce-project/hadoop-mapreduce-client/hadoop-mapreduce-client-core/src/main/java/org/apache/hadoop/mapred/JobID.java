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

/**
 * 代表MapReduce作业的不可变唯一标识符，用于在集群中唯一标识一个作业。
 * JobID由两部分组成：第一部分是JobTracker标识，集群模式下为JobTracker启动时间，本地模式下为"local"；
 * 第二部分是作业编号，代表该JobTracker启动后运行的第几个作业。
 * 示例：job_200707121733_0003 表示JobTracker在200707121733启动后运行的第三个作业。
 * <p>
 * 应用程序不应自行构造或解析JobID字符串，应使用提供的构造器或{@link #forName(String)}方法。
 * 
 * @see TaskID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JobID extends org.apache.hadoop.mapreduce.JobID {
  /**
   * 构造JobID对象
   * @param jtIdentifier jobTracker标识
   * @param id 作业编号
   */
  public JobID(String jtIdentifier, int id) {
    super(jtIdentifier, id);
  }
  
  public JobID() { }

  /**
   * 将新版本org.apache.hadoop.mapreduce.JobID降级转换为旧版本mapred.JobID，兼容旧API
   * @param old 新版本或旧版本的JobID实例
   * @return 转换后的旧版本JobID实例，如果输入已经是旧版本则直接返回
   */
  public static JobID downgrade(org.apache.hadoop.mapreduce.JobID old) {
    if (old instanceof JobID) {
      return (JobID) old;
    } else {
      return new JobID(old.getJtIdentifier(), old.getId());
    }
  }

  /**
   * 从DataInput反序列化读取构造JobID，已废弃
   * @param in 数据输入流
   * @return 反序列化得到的JobID
   * @throws IOException 读取失败时抛出IO异常
   */
  @Deprecated
  public static JobID read(DataInput in) throws IOException {
    JobID jobId = new JobID();
    jobId.readFields(in);
    return jobId;
  }

  /**
   * 从给定的字符串构造JobID对象
   * @param str 待解析的JobID字符串
   * @return 构造完成的JobID对象，输入为null时返回null
   * @throws IllegalArgumentException 如果输入字符串格式错误抛出异常
   */
  public static JobID forName(String str) throws IllegalArgumentException {
    return (JobID) org.apache.hadoop.mapreduce.JobID.forName(str);
  }
  
  /**
   * 获取匹配JobID的正则表达式，已废弃
   * @param jtIdentifier jobTracker标识，传入null表示该部分匹配任意值
   * @param jobId 作业编号，传入null表示该部分匹配任意值
   * @return 匹配符合条件JobID的正则表达式字符串
   */
  @Deprecated
  public static String getJobIDsPattern(String jtIdentifier, Integer jobId) {
    StringBuilder builder = new StringBuilder(JOB).append(SEPARATOR);
    builder.append(getJobIDsPatternWOPrefix(jtIdentifier, jobId));
    return builder.toString();
  }
  
  /**
   * 生成不包含前缀的JobID正则模式，已废弃
   * @param jtIdentifier jobTracker标识，传入null表示该部分匹配任意值
   * @param jobId 作业编号，传入null表示该部分匹配任意值
   * @return 包含正则模式的StringBuilder
   */
  @Deprecated
  static StringBuilder getJobIDsPatternWOPrefix(String jtIdentifier,
                                                Integer jobId) {
    StringBuilder builder = new StringBuilder();
    if (jtIdentifier != null) {
      builder.append(jtIdentifier);
    } else {
      // 匹配任意不包含分隔符的字符串
      builder.append("[^").append(SEPARATOR).append("]*");
    }
    // 拼接作业编号部分，指定编号则格式化输出，否则匹配任意数字
    builder.append(SEPARATOR)
      .append(jobId != null ? idFormat.format(jobId) : "[0-9]*");
    return builder;
  }

}