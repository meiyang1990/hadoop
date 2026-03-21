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

package org.apache.hadoop.yarn.server.timeline;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;

/**
 * 时间线数据写入接口，定义时间线服务存储实体和域数据的规范
 * 该接口为YARN应用时间线服务提供统一的数据写入抽象，不同存储后端可实现该接口
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface TimelineWriter {

  /**
   * 将时间线实体数据写入存储
   * 单个写入请求的错误会记录在返回响应中，不会直接抛出异常中断整个写入
   * 
   * @param data
   *          待写入的时间线实体集合
   * @return 写入响应，包含每个实体的写入结果（成功/错误信息）
   * @throws IOException 存储层面的IO异常
   */
  TimelinePutResponse put(TimelineEntities data) throws IOException;

  /**
   * 将时间线域信息写入存储
   * 如果同ID的域已存在，会完全覆盖原有域信息
   * 
   * @param domain
   *          待写入的时间线域对象
   * @throws IOException 存储层面的IO异常
   */
   void put(TimelineDomain domain) throws IOException;

}