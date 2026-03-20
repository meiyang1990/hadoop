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

package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;

/**
 * NodeManager 的 SharedCacheUploadService 与 SharedCacheManager 之间的协议，
 * 用于通知上传的共享缓存资源及查询是否允许上传。
 */
@Private
@Unstable
public interface SCMUploaderProtocol {
  /**
   * 汇报新上传的共享缓存资源，并由 SharedCacheManager 决定是否需要删除本地文件。
   */
  public SCMUploaderNotifyResponse
      notify(SCMUploaderNotifyRequest request)
      throws YarnException, IOException;

  /**
   * 询问指定资源是否允许上传到共享缓存，返回可否上传的决策结果。
   */
  public SCMUploaderCanUploadResponse
      canUpload(SCMUploaderCanUploadRequest request)
      throws YarnException, IOException;

}
