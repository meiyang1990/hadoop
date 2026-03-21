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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 存储被隔离异常，当RMStateStore被 fencing（ fencing机制用于脑裂防护，隔离旧Active RM）后抛出该异常
 * 用于资源管理器恢复流程中，标识当前状态存储已经被其他Active RM接管，当前节点无法继续操作存储
 */
public class StoreFencedException extends YarnException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造默认异常实例，错误信息固定为"RMStateStore has been fenced"
   */
  public StoreFencedException() {
    super("RMStateStore has been fenced");
  }
}