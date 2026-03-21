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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.yarn.webapp.Controller;

import com.google.inject.Inject;

/**
 * YARN Router Web UI 控制器，处理Router前端页面的请求分发与渲染
 */
public class RouterController extends Controller {

  @Inject
  RouterController(RequestContext ctx) {
    super(ctx);
  }

  @Override
  public void index() {
    setTitle("About the YARN Router");
    render(AboutPage.class);
  }

  /**
   * 处理集群信息页面请求，渲染关于页面
   */
  public void about() {
    setTitle("About the Cluster");
    render(AboutPage.class);
  }

  /**
   * 处理联邦信息页面请求，渲染联邦页面
   */
  public void federation() {
    render(FederationPage.class);
  }

  /**
   * 处理应用列表页面请求，渲染应用列表页面
   */
  public void apps() {
    setTitle("Applications");
    render(AppsPage.class);
  }

  /**
   * 处理节点列表页面请求，渲染节点列表页面
   */
  public void nodes() {
    setTitle("Nodes");
    render(NodesPage.class);
  }

  /**
   * 处理节点标签页面请求，渲染节点标签页面
   */
  public void nodeLabels() {
    setTitle("Node Labels");
    render(NodeLabelsPage.class);
  }
}