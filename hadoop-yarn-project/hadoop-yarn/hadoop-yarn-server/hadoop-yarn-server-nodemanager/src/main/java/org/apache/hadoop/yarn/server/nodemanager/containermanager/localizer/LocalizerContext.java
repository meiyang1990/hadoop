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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;

import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

/**
 * 本地化器上下文，保存容器本地化过程所需的上下文信息，
 * 包含用户信息、容器ID、安全凭证和文件状态缓存，用于容器资源本地化流程。
 */
public class LocalizerContext {

  private final String user;
  private final ContainerId containerId;
  private final Credentials credentials;
  private final LoadingCache<Path,Future<FileStatus>> statCache;

  /**
   * 构造不包含文件状态缓存的本地化器上下文。
   * @param user 提交容器的用户
   * @param containerId 容器ID
   * @param credentials 安全凭证
   */
  public LocalizerContext(String user, ContainerId containerId,
      Credentials credentials) {
    this(user, containerId, credentials, null);
  }

  /**
   * 构造完整的本地化器上下文，支持传入文件状态缓存。
   * @param user 提交容器的用户
   * @param containerId 容器ID
   * @param credentials 安全凭证，用于访问远程资源时的身份认证
   * @param statCache 文件状态缓存，缓存远程文件系统路径的状态信息，减少重复查询
   */
  public LocalizerContext(String user, ContainerId containerId,
      Credentials credentials,
      LoadingCache<Path,Future<FileStatus>> statCache) {
    this.user = user;
    this.containerId = containerId;
    this.credentials = credentials;
    this.statCache = statCache;
  }

  /**
   * 获取提交容器的用户名。
   * @return 用户名
   */
  public String getUser() {
    return user;
  }

  /**
   * 获取对应的容器ID。
   * @return 容器ID
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * 获取访问资源所需的安全凭证。
   * @return 安全凭证对象
   */
  public Credentials getCredentials() {
    return credentials;
  }

  /**
   * 获取文件状态缓存，用于缓存远程路径的FileStatus信息。
   * @return 文件状态缓存实例
   */
  public LoadingCache<Path,Future<FileStatus>> getStatCache() {
    return statCache;
  }
}