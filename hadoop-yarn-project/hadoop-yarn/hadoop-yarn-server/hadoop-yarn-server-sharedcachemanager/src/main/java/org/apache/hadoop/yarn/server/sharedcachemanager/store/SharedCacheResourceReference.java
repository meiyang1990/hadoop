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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * 文件概述: YARN共享缓存服务的资源引用实体类，存储应用对共享缓存资源的引用关系
 * 
 * 该类封装了共享缓存资源被哪个应用、哪个用户引用的元信息，用于统计资源引用计数，
 * 当没有任何应用引用资源时，共享缓存管理器会清理该资源以节省存储空间。
 */
@Private
@Evolving
public class SharedCacheResourceReference {
  // 引用该共享资源的应用ID
  private final ApplicationId appId;
  // 创建该引用的用户短名称
  private final String shortUserName;

  /**
   * 构造共享缓存资源引用对象
   * 
   * @param appId 引用资源的YARN应用ID
   * @param shortUserName 创建该引用的用户短名称
   */
  public SharedCacheResourceReference(ApplicationId appId, String shortUserName) {
    this.appId = appId;
    this.shortUserName = shortUserName;
  }

  /**
   * 获取引用该资源的应用ID
   * @return 应用ID
   */
  public ApplicationId getAppId() {
    return this.appId;
  }

  /**
   * 获取创建引用的用户短名称
   * @return 用户短名称
   */
  public String getShortUserName() {
    return this.shortUserName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    // 叠加应用ID的哈希值
    result = prime * result + ((appId == null) ? 0 : appId.hashCode());
    // 叠加用户名的哈希值
    result =
        prime * result
            + ((shortUserName == null) ? 0 : shortUserName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    // 同一对象直接返回相等
    if (this == obj)
      return true;
    // 比较对象为null直接返回不相等
    if (obj == null)
      return false;
    // 类型不同直接返回不相等
    if (getClass() != obj.getClass())
      return false;
    SharedCacheResourceReference other = (SharedCacheResourceReference) obj;
    // 比较应用ID是否相等
    if (appId == null) {
      if (other.appId != null)
        return false;
    } else if (!appId.equals(other.appId))
      return false;
    // 比较用户名是否相等
    if (shortUserName == null) {
      if (other.shortUserName != null)
        return false;
    } else if (!shortUserName.equals(other.shortUserName))
      return false;
    return true;
  }
}