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

package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * YARN服务端Web服务公共参数常量定义
 * 定义了可被ResourceManager、NodeManager、应用历史服务AHS等Web服务复用的公共请求参数常量
 */
@InterfaceAudience.LimitedPrivate({"YARN"})
public interface YarnWebServiceParams {

  // 容器日志相关Web服务使用的参数常量
  /** 应用ID参数名 */
  String APP_ID = "appid";
  /** 应用尝试ID参数名 */
  String APPATTEMPT_ID = "appattemptid";
  /** 容器ID参数名 */
  String CONTAINER_ID = "containerid";
  /** 容器日志文件名参数名 */
  String CONTAINER_LOG_FILE_NAME = "filename";
  /** 响应内容格式参数名 */
  String RESPONSE_CONTENT_FORMAT = "format";
  /** 响应内容大小参数名 */
  String RESPONSE_CONTENT_SIZE = "size";
  /** NodeManager节点ID参数名 */
  String NM_ID = "nm.id";
  /** 节点重定向来源参数名 */
  String REDIRECTED_FROM_NODE = "redirected_from_node";
  /** 集群ID参数名 */
  String CLUSTER_ID = "clusterid";
  /** 手动重定向标志参数名 */
  String MANUAL_REDIRECTION = "manual_redirection";
  /** 远程用户参数名 */
  String REMOTE_USER = "user";
  /** 文件大小参数名 */
  String FILESIZE = "file_size";
  /** 修改时间参数名 */
  String MODIFICATION_TIME = "modification_time";
}