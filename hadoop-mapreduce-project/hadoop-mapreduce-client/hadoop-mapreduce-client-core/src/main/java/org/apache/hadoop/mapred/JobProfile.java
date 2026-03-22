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
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.util.StringInterner;

/**
 * JobProfile类用于存储MapReduce作业的核心元数据信息，跟踪作业的基本属性，
 * 无论作业处于运行中还是已完成状态，都可以通过该类获取作业基本信息。
 * 是旧版MapReduce API中用于描述作业概况的核心数据结构。
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class JobProfile implements Writable {

  // 静态注册构造工厂，供Writable序列化框架反射创建实例
  static {
    WritableFactories.setFactory
      (JobProfile.class,
       new WritableFactory() {
         public Writable newInstance() { return new JobProfile(); }
       });
  }

  // 提交作业的用户名
  String user;
  // 作业唯一ID
  final JobID jobid;
  // 作业配置文件路径
  String jobFile;
  // 作业Web UI访问地址
  String url;
  // 作业名称
  String name;
  // 作业所属队列名称
  String queueName;
  
  /**
   * 构造空的JobProfile对象，供反序列化使用。
   */
  public JobProfile() {
    jobid = new JobID();
  }

  /**
   * 构造完整的JobProfile对象，包含作业核心元信息，默认使用默认队列。
   * 
   * @param user 提交作业的用户名
   * @param jobid 作业唯一ID
   * @param jobFile 作业配置文件路径
   * @param url 作业Web UI详情链接
   * @param name 用户指定的作业名称
   */
  public JobProfile(String user, org.apache.hadoop.mapreduce.JobID jobid, 
                    String jobFile, String url,
                    String name) {
    this(user, jobid, jobFile, url, name, JobConf.DEFAULT_QUEUE_NAME);
  }

  /**
   * 构造完整的JobProfile对象，包含作业核心元信息和队列信息。
   * 
   * @param user 提交作业的用户名
   * @param jobid 作业唯一ID
   * @param jobFile 作业配置文件路径
   * @param url 作业Web UI详情链接
   * @param name 用户指定的作业名称
   * @param queueName 作业提交到的队列名称
   */
  public JobProfile(String user, org.apache.hadoop.mapreduce.JobID jobid, 
                    String jobFile, String url,
                    String name, String queueName) {
    this.user = user;
    this.jobid = JobID.downgrade(jobid);
    this.jobFile = jobFile;
    this.url = url;
    this.name = name;
    this.queueName = queueName;
  }
  
  /**
   * 构造JobProfile对象，使用字符串格式作业ID。
   * @deprecated 已废弃，请使用JobProfile(String, JobID, String, String, String)替代
   */
  @Deprecated
  public JobProfile(String user, String jobid, String jobFile, String url,
      String name) {
    this(user, JobID.forName(jobid), jobFile, url, name);
  }
  
  /**
   * 获取提交作业的用户名。
   * @return 提交作业用户名
   */
  public String getUser() {
    return user;
  }
    
  /**
   * 获取作业唯一ID。
   * @return 作业ID对象
   */
  public JobID getJobID() {
    return jobid;
  }

  /**
   * 获取字符串格式的作业ID。
   * @deprecated 已废弃，请使用getJobID()替代
   * @return 字符串格式作业ID
   */
  @Deprecated
  public String getJobId() {
    return jobid.toString();
  }
  
  /**
   * 获取作业配置文件路径。
   * @return 作业配置文件路径
   */
  public String getJobFile() {
    return jobFile;
  }

  /**
   * 获取作业Web UI详情页URL。
   * @return 作业Web UI的URL对象，解析失败返回null
   */
  public URL getURL() {
    try {
      return new URL(url);
    } catch (IOException ie) {
      return null;
    }
  }

  /**
   * 获取用户指定的作业名称。
   * @return 作业名称
   */
  public String getJobName() {
    return name;
  }
  
  /**
   * 获取作业所属队列名称。
   * @return 队列名称
   */
  public String getQueueName() {
    return queueName;
  }
  
  ///////////////////////////////////////
  // Writable序列化接口实现
  ///////////////////////////////////////

  /**
   * 将JobProfile对象序列化输出到DataOutput流。
   * @param out 输出流
   * @throws IOException 输出过程IO异常
   */
  public void write(DataOutput out) throws IOException {
    jobid.write(out);
    Text.writeString(out, jobFile);
    Text.writeString(out, url);
    Text.writeString(out, user);
    Text.writeString(out, name);
    Text.writeString(out, queueName);
  }

  /**
   * 从DataInput流反序列化读取JobProfile对象数据。
   * 使用弱字符串驻留减少重复字符串内存占用。
   * @param in 输入流
   * @throws IOException 读取过程IO异常
   */
  public void readFields(DataInput in) throws IOException {
    jobid.readFields(in);
    this.jobFile = StringInterner.weakIntern(Text.readString(in));
    this.url = StringInterner.weakIntern(Text.readString(in));
    this.user = StringInterner.weakIntern(Text.readString(in));
    this.name = StringInterner.weakIntern(Text.readString(in));
    this.queueName = StringInterner.weakIntern(Text.readString(in));
  }
}