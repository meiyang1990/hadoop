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

/**
 * 资源本地化资源获取状态枚举，定义了节点管理器中资源下载获取的各类状态
 */
public enum ResourceStatusType {
  /** 资源获取等待中，尚未开始下载 */
  FETCH_PENDING,
  /** 资源获取成功完成 */
  FETCH_SUCCESS,
  /** 资源获取失败 */
  FETCH_FAILURE,
}