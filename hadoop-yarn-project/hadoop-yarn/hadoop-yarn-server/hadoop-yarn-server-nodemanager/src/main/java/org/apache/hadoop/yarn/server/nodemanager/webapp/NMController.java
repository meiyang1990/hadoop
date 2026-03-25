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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.inject.Inject;

/**
 * NodeManager Web UI控制器，负责处理NM页面请求路由
 * 接收不同路径的HTTP请求，渲染对应的Web页面
 */
public class NMController extends Controller implements YarnWebParams {
  
  @Inject
  public NMController(RequestContext requestContext) {
    super(requestContext);
  }

  @Override
  // TODO: What use of this with info() in?
  public void index() {
    // 设置页面标题，包含当前NodeManager节点名称
    setTitle(join("NodeManager - ", $(NM_NODENAME)));
  }

  /**
   * 处理NodeManager信息页请求，渲染节点信息页面
   */
  public void info() {
    render(NodePage.class);    
  }

  /**
   * 处理节点详情页请求，渲染节点信息页面
   */
  public void node() {
    render(NodePage.class);
  }

  /**
   * 处理所有应用列表请求，渲染NM上所有应用页面
   */
  public void allApplications() {
    render(AllApplicationsPage.class);
  }

  /**
   * 处理所有容器列表请求，渲染NM上所有容器页面
   */
  public void allContainers() {
    render(AllContainersPage.class);
  }

  /**
   * 处理单个应用详情请求，渲染指定应用详情页面
   */
  public void application() {
    render(ApplicationPage.class);
  }

  /**
   * 处理单个容器详情请求，渲染指定容器详情页面
   */
  public void container() {
    render(ContainerPage.class);
  }

  /**
   * 处理错误警告日志请求，渲染NM错误警告页面
   */
  public void errorsAndWarnings() {
    render(NMErrorsAndWarningsPage.class);
  }

  /**
   * 处理容器日志查看请求，渲染容器日志页面
   */
  public void logs() {
    render(ContainerLogsPage.class);
  }
}