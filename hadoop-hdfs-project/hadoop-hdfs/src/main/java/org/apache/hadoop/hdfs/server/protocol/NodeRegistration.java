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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 节点注册信息接口，定义DataNode/JournalNode等节点向NameNode注册时需要提供的基础信息
 */
@InterfaceAudience.Private
public interface NodeRegistration {
  /**
   * 获取节点的服务地址
   * @return 节点地址，格式为ipAddr:portNumber
   */
  public String getAddress();

  /**
   * 获取节点的注册ID
   * @return 节点注册ID，用于唯一标识该节点的本次注册
   */
  public String getRegistrationID();

  /**
   * 获取节点存储系统布局版本号
   * @return 节点布局版本号，用于兼容性检查
   */
  public int getVersion();

  @Override
  public String toString();
}