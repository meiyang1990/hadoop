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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 文件系统检查(Fsck)HTTP服务Servlet，运行在NameNode Web服务中，提供HDFS文件系统健康检查能力
 */
@InterfaceAudience.Private
public class FsckServlet extends DfsServlet {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

  /**
   * 处理HTTP GET请求，执行NameNode文件系统检查操作
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response
      ) throws IOException {
    @SuppressWarnings("unchecked")
    // 获取请求所有参数
    final Map<String,String[]> pmap = request.getParameterMap();
    // 获取响应输出流
    final PrintWriter out = response.getWriter();
    // 获取请求来源地址
    final InetAddress remoteAddress =
      InetAddress.getByName(request.getRemoteAddr());
    // 获取Servlet上下文
    final ServletContext context = getServletContext();
    // 从Servlet上下文中获取Hadoop配置
    final Configuration conf = NameNodeHttpServer.getConfFromContext(context);

    // 获取请求对应用户信息，用于权限认证
    final UserGroupInformation ugi = getUGI(request, conf);
    try {
      // 以请求用户身份执行fsck操作
      ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
        // 从Servlet上下文获取NameNode实例
        NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);

        // 获取NameNode文件系统管理器
        final FSNamesystem namesystem = nn.getNamesystem();
        // 获取块管理器
        final BlockManager bm = namesystem.getBlockManager();
        // 获取当前在线DataNode总数
        final int totalDatanodes =
            namesystem.getNumberOfDatanodes(DatanodeReportType.LIVE);
        // 创建fsck检查实例
        NamenodeFsck fsck = new NamenodeFsck(conf, nn,
            bm.getDatanodeManager().getNetworkTopology(), pmap, out,
            totalDatanodes, remoteAddress);
        // 获取审计日志来源标识
        String auditSource = fsck.getAuditSource();
        boolean success = false;
        try {
          // 执行文件系统检查
          fsck.fsck();
          success = true;
        } finally {
          // 记录fsck操作审计日志
          namesystem.logFsckEvent(success, auditSource, remoteAddress);
        }
        return null;
      });
    } catch (InterruptedException e) {
      // 操作被中断，返回400错误
      response.sendError(400, e.getMessage());
    }
  }
}