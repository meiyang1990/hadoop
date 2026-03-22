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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringInterner;

/**
 * 文件级注释：YARN作业队列ACL权限信息封装类，用于序列化传输特定用户对队列的可操作权限信息
 * 类级注释：封装特定用户对指定作业队列的访问控制列表(ACL)信息，支持Hadoop序列化机制
 * 用于在客户端和服务端之间传输队列权限信息
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QueueAclsInfo implements Writable {

  private String queueName;
  private String[] operations;
  
  /**
   * 构造方法：默认无参构造器，用于反序列化时创建对象
   */
  public QueueAclsInfo() {
    
  }

  /**
   * 构造方法：通过队列名称和允许的操作列表构建队列ACL信息对象
   * @param queueName 作业队列名称
   * @param operations 当前用户允许执行的操作列表
   */
  public QueueAclsInfo(String queueName, String[] operations) {
    this.queueName = queueName;
    this.operations = operations;    
  }

  /**
   * 获取队列名称
   * @return 作业队列名称
   */
  public String getQueueName() {
    return queueName;
  }

  protected void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * 获取当前用户允许在该队列执行的操作列表
   * @return 允许的操作字符串数组
   */
  public String[] getOperations() {
    return operations;
  }

  @Override
  /**
   * 反序列化：从输入流读取队列ACL信息
   */
  public void readFields(DataInput in) throws IOException {
    // 读取队列名称并进行字符串驻留优化内存
    queueName = StringInterner.weakIntern(Text.readString(in));
    // 读取允许操作列表
    operations = WritableUtils.readStringArray(in);
  }

  @Override
  /**
   * 序列化：将队列ACL信息写入输出流
   */
  public void write(DataOutput out) throws IOException {
    // 写入队列名称
    Text.writeString(out, queueName);
    // 写入允许操作列表
    WritableUtils.writeStringArray(out, operations);
  }
}