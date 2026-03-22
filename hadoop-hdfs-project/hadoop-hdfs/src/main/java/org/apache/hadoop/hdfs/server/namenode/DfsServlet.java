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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 文件所属模块：HDFS-NameNode服务端
 * DFS所有Web Servlet的抽象基类，提供通用基础能力，封装了身份信息获取等公共逻辑，
 * 供NameNode中各类具体业务Servlet继承实现，统一管理公共行为。
 */
public abstract class DfsServlet extends HttpServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  static final Logger LOG =
      LoggerFactory.getLogger(DfsServlet.class.getCanonicalName());

  /**
   * 从HTTP请求中获取请求用户的用户组信息，用于HDFS Web接口的权限认证
   * @param request HTTP请求对象
   * @param conf Hadoop配置对象
   * @return 请求对应用户的UserGroupInformation信息
   * @throws IOException 获取用户信息过程中发生IO异常时抛出
   */
  protected UserGroupInformation getUGI(HttpServletRequest request,
                                        Configuration conf) throws IOException {
    return JspHelper.getUGI(getServletContext(), request, conf);
  }
}