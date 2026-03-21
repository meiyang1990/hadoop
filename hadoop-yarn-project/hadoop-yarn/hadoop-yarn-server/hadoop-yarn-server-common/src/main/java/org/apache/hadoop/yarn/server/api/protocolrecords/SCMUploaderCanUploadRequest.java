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
 * NodeManager向共享缓存管理器SCM发起的请求，询问是否可以将指定资源上传到共享缓存
 * </p>
 */
@Private
@Unstable
public abstract class SCMUploaderCanUploadRequest {

  /**
   * 获取待上传到共享缓存的资源的唯一标识key
   * @return 资源唯一标识key
   */
  public abstract String getResourceKey();

  /**
   * 设置待上传到共享缓存的资源的唯一标识key
   * @param key 资源的唯一标识符
   */
  public abstract void setResourceKey(String key);
}