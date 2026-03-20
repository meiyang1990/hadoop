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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;

/**
 * TimelineCollectorManager 与 NodeManager 之间的协议接口，
 * 用于通知节点有新的应用级 collector 启动并同步上下文信息。
 */
@Private
public interface CollectorNodemanagerProtocol {

  /**
   * 注册新的应用 collector 信息（应用 ID 与 REST 访问地址），
   * 让 NodeManager 建立本地映射并随后汇报给 ResourceManager。
   */
  ReportNewCollectorInfoResponse reportNewCollectorInfo(
      ReportNewCollectorInfoRequest request)
      throws YarnException, IOException;

  /**
   * 供 collector 查询应用的上下文（用户、flow、flow run），
   * 以便在后续 put-entity 请求中附带正确关联信息。
   */
  GetTimelineCollectorContextResponse getTimelineCollectorContext(
      GetTimelineCollectorContextRequest request)
      throws YarnException, IOException;
}
