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
import org.apache.hadoop.yarn.util.Records;

/**
 * 上报新采集器信息响应，YARN服务端内部协议的响应记录类
 * 用于接收Timeline服务采集器上线注册的响应结果
 */
public abstract class ReportNewCollectorInfoResponse {

  /**
   * 创建ReportNewCollectorInfoResponse新实例
   * @return 新的响应对象实例
   */
  @Private
  public static ReportNewCollectorInfoResponse newInstance() {
    ReportNewCollectorInfoResponse response =
        Records.newRecord(ReportNewCollectorInfoResponse.class);
    return response;
  }

}