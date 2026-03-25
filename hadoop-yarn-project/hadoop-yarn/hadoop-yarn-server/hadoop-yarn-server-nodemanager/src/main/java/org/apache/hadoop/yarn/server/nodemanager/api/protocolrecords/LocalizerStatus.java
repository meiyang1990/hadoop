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
package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords;

import java.util.List;

/**
 * 本地化器状态接口，用于NodeManager与资源本地化服务之间传递本地化器状态信息
 * 记录了当前本地化器正在处理的所有资源的本地化进度状态
 */
public interface LocalizerStatus {

  /**
   * 获取本地化器唯一标识ID
   * @return 本地化器ID
   */
  String getLocalizerId();

  /**
   * 设置本地化器唯一标识ID
   * @param id 本地化器ID
   */
  void setLocalizerId(String id);

  /**
   * 获取所有正在本地化的资源状态列表
   * @return 资源状态列表
   */
  List<LocalResourceStatus> getResources();

  /**
   * 批量添加多个资源状态
   * @param resources 待添加的资源状态列表
   */
  void addAllResources(List<LocalResourceStatus> resources);

  /**
   * 添加单个资源状态
   * @param resource 待添加的资源状态
   */
  void addResourceStatus(LocalResourceStatus resource);

  /**
   * 根据索引获取指定位置的资源状态
   * @param index 列表索引
   * @return 对应索引的资源状态
   */
  LocalResourceStatus getResourceStatus(int index);

  /**
   * 移除指定索引位置的资源状态
   * @param index 列表索引
   */
  void removeResource(int index);

  /**
   * 清空所有资源状态
   */
  void clearResources();
}