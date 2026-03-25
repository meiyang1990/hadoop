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

/**
 * YARN 共享缓存服务资源存储模块，定义共享缓存资源实体类
 */
package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

// 共享缓存资源封装类。实例非线程安全，使用时必须通过线程安全机制确保访问安全（文件名除外）
/**
 * 封装共享缓存中单个资源的元数据信息。本类实例非线程安全，除文件名外，所有对资源的访问操作必须使用外部线程安全机制保证访问安全。
 */
@Private
@Evolving
class SharedCacheResource {
  // 资源最后访问时间戳
  private long accessTime;
  // 当前资源的所有引用集合，记录哪些应用正在使用该资源
  private final Set<SharedCacheResourceReference> refs;
  // 资源在本地文件系统中的存储文件名
  private final String fileName;

  /**
   * 构造共享缓存资源对象，初始化访问时间为当前时间
   * @param fileName 资源存储文件名
   */
  SharedCacheResource(String fileName) {
    this.accessTime = System.currentTimeMillis();
    this.refs = new HashSet<SharedCacheResourceReference>();
    this.fileName = fileName;
  }

  /**
   * 获取资源最后访问时间
   * @return 最后访问时间戳
   */
  long getAccessTime() {
    return accessTime;
  }

  /**
   * 将资源最后访问时间更新为当前时间
   */
  void updateAccessTime() {
    accessTime = System.currentTimeMillis();
  }

  /**
   * 获取资源存储文件名
   * @return 本地存储文件名
   */
  String getFileName() {
    return this.fileName;
  }

  /**
   * 获取当前资源的所有引用集合
   * @return 资源引用集合
   */
  Set<SharedCacheResourceReference> getResourceReferences() {
    return this.refs;
  }

  /**
   * 添加一个资源引用，标记该资源被某应用使用
   * @param ref 资源引用对象
   * @return 添加成功返回true，引用已存在返回false
   */
  boolean addReference(SharedCacheResourceReference ref) {
    return this.refs.add(ref);
  }
}