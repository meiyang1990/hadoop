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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * YARN ResourceManager 调度器容器更新请求持有者，按类型分类维护所有待处理的容器更新请求
 */
public class ContainerUpdates {

  // 容器资源增加请求列表
  final List<UpdateContainerRequest> increaseRequests = new ArrayList<>();
  // 容器资源减少请求列表
  final List<UpdateContainerRequest> decreaseRequests = new ArrayList<>();
  // 容器优先级提升请求列表
  final List<UpdateContainerRequest> promotionRequests = new ArrayList<>();
  // 容器优先级降级请求列表
  final List<UpdateContainerRequest> demotionRequests = new ArrayList<>();

  /**
   * 获取所有容器资源增加请求
   * @return 容器资源增加请求列表
   */
  public List<UpdateContainerRequest> getIncreaseRequests() {
    return increaseRequests;
  }

  /**
   * 获取所有容器资源减少请求
   * @return 容器资源减少请求列表
   */
  public List<UpdateContainerRequest> getDecreaseRequests() {
    return decreaseRequests;
  }

  /**
   * 获取所有容器优先级提升请求
   * @return 容器优先级提升请求列表
   */
  public List<UpdateContainerRequest> getPromotionRequests() {
    return promotionRequests;
  }

  /**
   * 获取所有容器优先级降级请求
   * @return 容器优先级降级请求列表
   */
  public List<UpdateContainerRequest> getDemotionRequests() {
    return demotionRequests;
  }

}