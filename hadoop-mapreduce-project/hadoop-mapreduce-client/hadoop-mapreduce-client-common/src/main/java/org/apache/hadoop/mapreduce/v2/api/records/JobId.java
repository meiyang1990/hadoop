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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.text.NumberFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * <p><code>JobId</code> represents the <em>globally unique</em> 
 * identifier for a MapReduce job.</p>
 * 
 * <p>The globally unique nature of the identifier is achieved by using the 
 * <em>cluster timestamp</em> from the associated ApplicationId. i.e. 
 * start-time of the <code>ResourceManager</code> along with a monotonically
 * increasing counter for the jobId.</p>
 * 
 *  MapReduce作业的全局唯一标识符，基于关联的ApplicationId和自增作业序号生成全局唯一性
 */
public abstract class JobId implements Comparable<JobId> {

  /**
   * Get the associated <em>ApplicationId</em> which represents the 
   * start time of the <code>ResourceManager</code> and is used to generate 
   * the globally unique <code>JobId</code>.
   * @return associated <code>ApplicationId</code>
   * 获取该作业所属YARN应用的ApplicationId，用于保证全局唯一性
   */
  public abstract ApplicationId getAppId();
  
  /**
   * Get the short integer identifier of the <code>JobId</code>
   * which is unique for all applications started by a particular instance
   * of the <code>ResourceManager</code>.
   * @return short integer identifier of the <code>JobId</code>
   * 获取当前ResourceManager实例内唯一的作业序号
   */
  public abstract int getId();
  
  /**
   * 设置该作业所属YARN应用的ApplicationId
   * @param appId 所属应用的ApplicationId
   */
  public abstract void setAppId(ApplicationId appId);

  /**
   * 设置作业序号
   * @param id 当前ResourceManager实例内唯一的作业序号
   */
  public abstract void setId(int id);


  // 作业标识前缀
  protected static final String JOB = "job";
  // 分隔符
  protected static final char SEPARATOR = '_';
  // 线程本地的NumberFormat，用于格式化作业序号为固定4位长度，避免多线程竞争
  static final ThreadLocal<NumberFormat> jobIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          // 不使用分组分隔符
          fmt.setGroupingUsed(false);
          // 最小位数设为4，不足补零
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  @Override
  /**
   * 生成JobId的标准字符串表示，格式为job_{集群时间戳}_{4位作业序号}
   */
  public String toString() {
    StringBuilder builder = new StringBuilder(JOB);
    builder.append(SEPARATOR);
    // 追加ResourceManager启动时间戳
    builder.append(getAppId().getClusterTimestamp());
    builder.append(SEPARATOR);
    // 追加格式化后的作业序号
    builder.append(jobIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  /**
   * 计算JobId的哈希值，基于ApplicationId和作业序号
   */
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getAppId().hashCode();
    result = prime * result + getId();
    return result;
  }

  @Override
  /**
   * 判断两个JobId是否相等，基于ApplicationId和作业序号完全匹配
   */
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JobId other = (JobId) obj;
    if (!this.getAppId().equals(other.getAppId()))
      return false;
    if (this.getId() != other.getId())
      return false;
    return true;
  }

  @Override
  /**
   * 按ApplicationId比较排序，ApplicationId相同时按作业序号排序
   */
  public int compareTo(JobId other) {
    int appIdComp = this.getAppId().compareTo(other.getAppId());
    if (appIdComp == 0) {
      // 应用ID相同，比较作业序号
      return this.getId() - other.getId();
    } else {
      return appIdComp;
    }
  }
}