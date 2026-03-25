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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * NameNode可检查资源接口，定义资源可用性检查和资源必要性判断的统一契约。
 * 该接口用于对NameNode运行依赖的各类资源进行健康检查，根据资源性质分为必需资源和冗余资源：
 * 所有必需资源必须可用，NameNode才能继续运行；只要存在任意一个冗余资源可用，NameNode即可继续运行。
 */
@InterfaceAudience.Private
interface CheckableNameNodeResource {
  
  /**
   * 检查当前资源是否可用。
   * 
   * @return true 资源可用，false 资源不可用
   */
  public boolean isResourceAvailable();
  
  /**
   * 判断当前资源是否为NameNode运行的必需资源。
   * 
   * @return true 是必需资源，false 是冗余资源
   */
  public boolean isRequired();

}