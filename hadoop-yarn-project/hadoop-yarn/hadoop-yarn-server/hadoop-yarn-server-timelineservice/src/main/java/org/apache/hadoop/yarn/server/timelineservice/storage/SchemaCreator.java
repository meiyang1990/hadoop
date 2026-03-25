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

package org.apache.hadoop.yarn.server.timelineservice.storage;

/**
 * 时间线服务存储层Schema创建接口，定义了创建时间线服务底层存储Schema的统一规范。
 * 不同存储后端的时间线服务实现都需要实现该接口，完成存储Schema的初始化。
 */
public interface SchemaCreator {

  /**
   * 创建时间线服务存储Schema。
   * @param args 创建Schema所需的命令行参数
   * @throws Exception 创建Schema过程中抛出的异常
   */
  void createTimelineSchema(String[] args) throws Exception;
}