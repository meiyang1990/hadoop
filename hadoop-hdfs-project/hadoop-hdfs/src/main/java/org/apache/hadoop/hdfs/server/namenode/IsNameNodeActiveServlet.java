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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.http.IsActiveServlet;

/**
 * HDFS NameNode节点活跃状态查询Servlet
 * 供负载均衡器探测当前NameNode是否为集群中的活跃节点，用于高可用场景下的流量路由
 */
public class IsNameNodeActiveServlet extends IsActiveServlet {

  /**
   * 判断当前NameNode节点是否处于活跃状态
   * @return true表示当前NameNode为活跃状态，false表示为 standby 状态
   */
  @Override
  protected boolean isActive() {
    // 从Servlet上下文获取当前节点的NameNode实例
    NameNode namenode = NameNodeHttpServer.getNameNodeFromContext(
        getServletContext());
    // 返回NameNode当前的活跃状态
    return namenode.isActiveState();
  }
}