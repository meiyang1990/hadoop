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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 作业基础信息存储类，用于JobTracker接收到作业提交请求时，保存作业元信息
 * 核心作用是在JobTracker重启后，能够恢复未完成的作业，实现故障恢复
 */
class JobInfo implements Writable {
  private org.apache.hadoop.mapreduce.JobID id;
  private Text user;
  private Path jobSubmitDir;
  public JobInfo() {}
  
  /**
   * 构造JobInfo对象，封装作业基础元信息
   * @param id 作业ID
   * @param user 提交作业的用户名
   * @param jobSubmitDir 作业提交目录路径
   */
  public JobInfo(org.apache.hadoop.mapreduce.JobID id, 
      Text user,
      Path jobSubmitDir) {
    this.id = id;
    this.user = user;
    this.jobSubmitDir = jobSubmitDir;
  }
  
  /**
   * 获取作业ID
   * @return 作业ID对象
   */
  public org.apache.hadoop.mapreduce.JobID getJobID() {
    return id;
  }
  
  /**
   * 获取提交作业的用户名
   * @return 提交用户的用户名
   */
  public Text getUser() {
    return user;
  }
      
  /**
   * 获取作业提交目录路径
   * @return 作业提交目录路径
   */
  public Path getJobSubmitDir() {
    return this.jobSubmitDir;
  }
  
  /**
   * 从二进制输入流反序列化JobInfo对象
   * @param in 二进制输入流
   * @throws IOException 反序列化过程中发生IO异常
   */
  public void readFields(DataInput in) throws IOException {
    id = new org.apache.hadoop.mapreduce.JobID();
    id.readFields(in);
    user = new Text();
    user.readFields(in);
    jobSubmitDir = new Path(WritableUtils.readString(in));
  }

  /**
   * 将JobInfo对象序列化到二进制输出流
   * @param out 二进制输出流
   * @throws IOException 序列化过程中发生IO异常
   */
  public void write(DataOutput out) throws IOException {
    id.write(out);
    user.write(out);
    WritableUtils.writeString(out, jobSubmitDir.toString());
  }
}