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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

/**
 * 文件说明: MapReduce任务（Job）的唯一标识符，用于在集群中唯一标识一个作业
 * 
 * JobID represents the immutable and unique identifier for 
 * the job. JobID consists of two parts. First part 
 * represents the jobtracker identifier, so that jobID to jobtracker map 
 * is defined. For cluster setup this string is the jobtracker 
 * start time, for local setting, it is "local" and a random number.
 * Second part of the JobID is the job number. <br> 
 * An example JobID is : 
 * <code>job_200707121733_0003</code> , which represents the third job 
 * running at the jobtracker started at <code>200707121733</code>. 
 * <p>
 * Applications should never construct or parse JobID strings, but rather 
 * use appropriate constructors or {@link #forName(String)} method. 
 * 
 * @see TaskID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * JobID类，用于生成和管理MapReduce作业的全局唯一标识
 * 由作业Tracker标识符和作业编号两部分组成，不可变
 */
public class JobID extends org.apache.hadoop.mapred.ID 
                   implements Comparable<ID> {
  // JobID的前缀标识
  public static final String JOB = "job";
  
  // 用于匹配JobID格式的正则表达式，供框架和工具解析使用
  public static final String JOBID_REGEX = 
    JOB + SEPARATOR + "[0-9]+" + SEPARATOR + "[0-9]+";
  
  // JobTracker标识符，区分不同JobTracker实例
  private final Text jtIdentifier;
  
  // 作业编号格式化工具，保证编号固定4位长度
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    // 禁用千分位分组
    idFormat.setGroupingUsed(false);
    // 最少4位数字，不足补零
    idFormat.setMinimumIntegerDigits(4);
  }
  
  /**
   * 构造JobID对象
   * @param jtIdentifier jobTracker标识符
   * @param id 作业编号
   */
  public JobID(String jtIdentifier, int id) {
    super(id);
    this.jtIdentifier = new Text(jtIdentifier);
  }
  
  /**
   * 空构造方法，用于反序列化场景
   */
  public JobID() { 
    jtIdentifier = new Text();
  }
  
  /**
   * 获取JobTracker标识符
   * @return 返回当前JobID对应的JobTracker标识字符串
   */
  public String getJtIdentifier() {
    return jtIdentifier.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    JobID that = (JobID)o;
    return this.jtIdentifier.equals(that.jtIdentifier);
  }
  
  /**
   * 比较规则：先比较JobTracker标识符，再比较作业编号
   */
  @Override
  public int compareTo(ID o) {
    JobID that = (JobID)o;
    int jtComp = this.jtIdentifier.compareTo(that.jtIdentifier);
    if(jtComp == 0) {
      return this.id - that.id;
    }
    else return jtComp;
  }
  
  /**
   * 将JobID中"job"前缀之后的部分追加到StringBuilder中
   * 供TaskID等子标识复用格式，子标识会复用JobID的部分作为前缀
   * @param builder 要追加内容的StringBuilder
   * @return 追加后的StringBuilder
   */
  public StringBuilder appendTo(StringBuilder builder) {
    builder.append(SEPARATOR);
    builder.append(jtIdentifier);
    builder.append(SEPARATOR);
    builder.append(idFormat.format(id));
    return builder;
  }

  @Override
  public int hashCode() {
    return jtIdentifier.hashCode() + id;
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(JOB)).toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.jtIdentifier.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jtIdentifier.write(out);
  }
  
  /** 
   * 从字符串解析构造JobID对象
   * @return 解析得到的JobID对象，输入为null时返回null
   * @throws IllegalArgumentException 如果字符串格式不符合要求则抛出异常
   */
  public static JobID forName(String str) throws IllegalArgumentException {
    if(str == null)
      return null;
    try {
      String[] parts = str.split("_");
      if(parts.length == 3) {
        if(parts[0].equals(JOB)) {
          return new org.apache.hadoop.mapred.JobID(parts[1], 
                                                    Integer.parseInt(parts[2]));
        }
      }
    }catch (Exception ex) {//fall below
    }
    throw new IllegalArgumentException("JobId string : " + str 
        + " is not properly formed");
  }
  
}