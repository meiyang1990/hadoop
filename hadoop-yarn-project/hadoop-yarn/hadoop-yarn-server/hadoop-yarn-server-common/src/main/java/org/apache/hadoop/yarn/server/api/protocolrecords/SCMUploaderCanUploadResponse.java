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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>
 * 共享缓存管理器(SCM)返回给NodeManager的响应，通知NodeManager是否可以将资源上传到共享缓存。
 * 如果SCM不允许上传，NodeManager不应将资源上传到共享缓存。
 * </p>
 */
@Private
@Unstable
public abstract class SCMUploaderCanUploadResponse {

  /**
   * 获取NodeManager是否可以将资源上传到共享缓存
   *
   * @return boolean 允许上传返回true，否则返回false
   */
  public abstract boolean getUploadable();

  /**
   * 设置NodeManager是否可以将资源上传到共享缓存
   *
   * @param b 允许上传传true，否则传false
   */
  public abstract void setUploadable(boolean b);

}