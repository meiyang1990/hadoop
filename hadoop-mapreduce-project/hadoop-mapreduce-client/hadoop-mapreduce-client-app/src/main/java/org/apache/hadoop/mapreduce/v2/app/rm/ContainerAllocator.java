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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.event.EventHandler;

/**
 * 容器分配器接口，定义MapReduce ApplicationMaster向YARN申请、释放容器的核心能力
 * 负责处理容器分配相关事件，对接YARN ResourceManager完成资源分配
 */
public interface ContainerAllocator extends EventHandler<ContainerAllocatorEvent>{

  /**
   * 容器分配事件类型枚举，定义所有支持的容器操作事件类型
   */
  enum EventType {
    /** 请求分配新容器 */
    CONTAINER_REQ,
    /** 释放不再使用的容器 */
    CONTAINER_DEALLOCATE,
    /** 标记容器分配失败 */
    CONTAINER_FAILED
  }

}