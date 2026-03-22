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

/**
 * 文件说明：旧版MapReduce API中作业队列访问控制列表信息封装类
 * 用于封装特定用户对指定作业队列的访问权限信息，兼容旧版mapred API
 */
/**
 * 封装特定用户的作业队列访问控制列表(ACL)信息
 * 是新版QueueAclsInfo在旧版mapred API中的兼容扩展类
 */
class QueueAclsInfo extends org.apache.hadoop.mapreduce.QueueAclsInfo {

  /**
   * 构造方法：创建空的队列ACL信息对象
   */
  QueueAclsInfo() {
    super();
  }

  /**
   * 构造方法：根据队列名称和允许的操作列表创建队列ACL信息对象
   * 
   * @param queueName 作业队列名称
   * @param operations 允许当前用户执行的队列操作数组
   * 
   */
  QueueAclsInfo(String queueName, String[] operations) {
    super(queueName, operations);
  }
  
  /**
   * 将新版mapreduce API的QueueAclsInfo对象降级为旧版mapred API的QueueAclsInfo对象
   * 用于API版本兼容转换，适配旧版接口的调用需求
   * 
   * @param acl 新版API的队列ACL信息对象
   * @return 转换后的旧版API队列ACL信息对象
   */
  public static QueueAclsInfo downgrade(
      org.apache.hadoop.mapreduce.QueueAclsInfo acl) {
    return new QueueAclsInfo(acl.getQueueName(), acl.getOperations());
  }
}