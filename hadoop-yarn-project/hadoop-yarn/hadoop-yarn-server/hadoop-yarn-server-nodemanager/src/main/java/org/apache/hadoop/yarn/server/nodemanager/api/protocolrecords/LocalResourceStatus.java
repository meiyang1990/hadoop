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

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.URL;

/**
 * 本地资源状态记录接口，用于NodeManager与外部组件通信时传递资源本地化状态信息
 * 记录了容器运行所需本地资源的下载状态、本地路径、大小和异常信息
 */
public interface LocalResourceStatus {
  /**
   * 获取对应的本地资源信息
   * @return 本地资源对象
   */
  public LocalResource getResource();
  /**
   * 获取资源本地化状态
   * @return 资源状态枚举类型
   */
  public ResourceStatusType getStatus();
  /**
   * 获取资源本地化后的本地路径
   * @return 本地文件路径URL
   */
  public URL getLocalPath();
  /**
   * 获取本地化后资源文件大小
   * @return 资源大小，单位字节
   */
  public long getLocalSize();
  /**
   * 获取资源本地化过程中出现的异常
   * @return 序列化后的异常对象
   */
  public SerializedException getException();

  /**
   * 设置对应的本地资源信息
   * @param resource 本地资源对象
   */
  public void setResource(LocalResource resource);
  /**
   * 设置资源本地化状态
   * @param status 资源状态枚举类型
   */
  public void setStatus(ResourceStatusType status);
  /**
   * 设置资源本地化后的本地路径
   * @param localPath 本地文件路径URL
   */
  public void setLocalPath(URL localPath);
  /**
   * 设置本地化后资源文件大小
   * @param size 资源大小，单位字节
   */
  public void setLocalSize(long size);
  /**
   * 设置资源本地化过程中出现的异常
   * @param exception 序列化后的异常对象
   */
  public void setException(SerializedException exception);
}