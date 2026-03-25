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
 * 共享缓存缓存管理器(SCM)返回给NodeManager的通知响应，告知NodeManager是否需要删除本次通知对应的缓存资源。
 * </p>
 * 该响应用于NodeManager上传资源到共享缓存后的结果确认流程。
 */
@Private
@Unstable
public abstract class SCMUploaderNotifyResponse {

  /**
   * 获取共享缓存管理器是否接受了本次通知的资源。
   * 如果接受，则已上传的文件应保留在缓存中；否则NodeManager需要删除该资源。
   *
   * @return boolean 接受资源返回true，需要删除资源返回false
   */
  public abstract boolean getAccepted();

  /**
   * 设置共享缓存管理器对本次通知资源的接受状态。
   *
   * @param b 接受资源为true，拒绝资源为false
   */
  public abstract void setAccepted(boolean b);

}