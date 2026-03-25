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
import java.text.NumberFormat;

/**
 * JVM进程唯一标识符，用于标识MapReduce任务执行容器中的JVM进程
 * 支持MR作业重用JVM机制，记录每个JVM所属作业、任务类型和唯一编号
 */
class JVMId {
  boolean isMap;
  final JobID jobId;
  private long jvmId;
  private static final String JVM = "jvm";
  private static final char SEPARATOR = '_';
  private static NumberFormat idFormat = NumberFormat.getInstance();
  // 静态初始化JVM ID格式器
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }
  
  /**
   * 构造指定作业、任务类型和ID的JVM标识符
   * @param jobId 所属作业ID
   * @param isMap 是否为Map任务JVM
   * @param id JVM唯一编号
   */
  public JVMId(JobID jobId, boolean isMap, long id) {
    this.jvmId = id;
    this.isMap = isMap;
    this.jobId = jobId;
  }
  
  /**
   * 通过作业标识字符串和作业ID构造JVM标识符
   * @param jtIdentifier 作业跟踪器标识
   * @param jobId 作业编号
   * @param isMap 是否为Map任务JVM
   * @param id JVM唯一编号
   */
  public JVMId (String jtIdentifier, int jobId, boolean isMap, long id) {
    this(new JobID(jtIdentifier, jobId), isMap, id);
  }
    
  /**
   * 空构造方法，用于反序列化场景
   */
  public JVMId() { 
    jobId = new JobID();
  }
  
  /**
   * 判断当前JVM是否为Map任务JVM
   * @return true表示Map任务JVM，false表示Reduce任务JVM
   */
  public boolean isMapJVM() {
    return isMap;
  }

  /**
   * 获取当前JVM所属作业ID
   * @return 所属作业ID对象
   */
  public JobID getJobId() {
    return jobId;
  }

  @Override
  public boolean equals(Object o) {
    // 引用相同直接返回true
    if (this == o) {
      return true;
    }
    // 类型不同返回false
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JVMId jvmId1 = (JVMId) o;

    // 比较任务类型
    if (isMap != jvmId1.isMap) {
      return false;
    }
    // 比较JVM编号
    if (jvmId != jvmId1.jvmId) {
      return false;
    }
    // 比较所属作业ID
    if (!jobId.equals(jvmId1.jobId)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    // 计算哈希值，采用标准的哈希组合算法
    int result = (isMap ? 1 : 0);
    result = 31 * result + jobId.hashCode();
    result = 31 * result + (int) (jvmId ^ (jvmId >>> 32));
    return result;
  }

  /**
   * 按规则比较两个JVMId的大小：先比较作业ID，再比较任务类型，最后比较JVM编号
   * Map任务JVM小于Reduce任务JVM
   * @param that 待比较的另一个JVMId
   * @return 比较结果，小于0表示当前更小，等于0表示相等，大于0表示当前更大
   **/
  public int compareTo(JVMId that) {
    int jobComp = this.jobId.compareTo(that.jobId);
    if(jobComp == 0) {
      if(this.isMap == that.isMap) {
        return Long.compare(this.jvmId, that.jvmId);
      } else {
        return this.isMap ? -1 : 1;
      }
    } else {
      return jobComp;
    }
  }
  
  @Override
  public String toString() { 
    return appendTo(new StringBuilder(JVM)).toString();
  }

  /**
   * 获取JVM的64位唯一编号，支持RM重启时的工作保留
   * @return 64位JVM编号
   */
  public long getId() {
    return jvmId;
  }

  /**
   * 将JVMId信息追加到指定StringBuilder
   * @param builder 待追加的StringBuilder
   * @return 追加后的StringBuilder
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return jobId.appendTo(builder).
                 append(SEPARATOR).
                 append(isMap ? 'm' : 'r').
                 append(SEPARATOR).
                 append(idFormat.format(jvmId));
  }

  /**
   * 从输入流反序列化JVMId对象
   * @param in 输入数据流
   * @throws IOException 读取数据时发生IO异常
   */
  public void readFields(DataInput in) throws IOException {
    this.jvmId = in.readLong();
    this.jobId.readFields(in);
    this.isMap = in.readBoolean();
  }

  /**
   * 将JVMId对象序列化到输出流
   * @param out 输出数据流
   * @throws IOException 写入数据时发生IO异常
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(jvmId);
    jobId.write(out);
    out.writeBoolean(isMap);
  }
  
  /**
   * 从字符串解析构造JVMId对象
   * @param str 待解析的JVMId字符串
   * @return 解析后的JVMId对象，输入为null时返回null
   * @throws IllegalArgumentException 输入字符串格式错误时抛出异常
   */
  public static JVMId forName(String str) 
    throws IllegalArgumentException {
    if(str == null)
      return null;
    try {
      // 按下划线分割字符串
      String[] parts = str.split("_");
      if(parts.length == 5) {
        // 验证前缀是否为jvm
        if(parts[0].equals(JVM)) {
          boolean isMap = false;
          // 解析任务类型
          if(parts[3].equals("m")) isMap = true;
          else if(parts[3].equals("r")) isMap = false;
          else throw new Exception();
          return new JVMId(parts[1], Integer.parseInt(parts[2]),
              isMap, Integer.parseInt(parts[4]));
        }
      }
    }catch (Exception ex) {//解析异常，向下抛出格式错误
    }
    throw new IllegalArgumentException("TaskId string : " + str 
        + " is not properly formed");
  }

}