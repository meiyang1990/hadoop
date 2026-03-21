// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.servlet.http.HttpServletRequest;

/**
 * YARN WebUI 应用信息提供者接口，被{@link LogServlet}用于获取应用相关信息。
 * 实现类负责从不同数据源（RM/NM）查询应用、容器相关信息，为日志等Web请求提供数据支撑。
 */
@InterfaceAudience.LimitedPrivate({"YARN"})
@InterfaceStability.Unstable
public interface AppInfoProvider {

  /**
   * 根据请求信息获取容器所在节点的HTTP访问地址
   *
   * @param req HTTP请求对象
   * @param appId 应用ID
   * @param appAttemptId 应用尝试ID
   * @param containerId 容器ID
   * @param clusterId 集群ID
   * @return 节点HTTP地址
   */
  String getNodeHttpAddress(HttpServletRequest req,
      String appId, String appAttemptId, String containerId, String clusterId);

  /**
   * 根据应用ID获取应用基本信息
   *
   * @param req HTTP请求对象
   * @param appId 应用ID
   * @param clusterId 集群ID
   * @return 封装好的应用基本信息对象
   */
  BasicAppInfo getApp(HttpServletRequest req, String appId, String clusterId);
}