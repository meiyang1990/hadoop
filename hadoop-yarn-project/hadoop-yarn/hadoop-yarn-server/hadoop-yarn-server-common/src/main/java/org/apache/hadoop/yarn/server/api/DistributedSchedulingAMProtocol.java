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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * 扩展 ApplicationMasterProtocol，供运行在 NodeManager 上的 DistributedScheduler 使用，
 * 在注册和分配 RPC 中封装分布式调度所需的额外元数据。
 */
public interface DistributedSchedulingAMProtocol
    extends ApplicationMasterProtocol {

  /**
   * 扩展 registerApplicationMaster，返回包含分布式调度附加信息的注册响应。
   */
  @Public
  @Unstable
  @Idempotent
  RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
            RegisterApplicationMasterRequest request)
            throws YarnException, IOException;

  /**
   * 扩展 allocate，返回带有分布式调度附加信息的分配响应。
   */
  @Public
  @Unstable
  @Idempotent
  DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException;
}
