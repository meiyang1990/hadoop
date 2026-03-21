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

package org.apache.hadoop.yarn.server.sharedcachemanager.webapp;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.webapp.Controller;

/**
 * 共享缓存管理器Web应用的请求控制器，负责处理Web页面请求并渲染对应页面
 */
@Private
@Unstable
public class SCMController extends Controller {
  @Override
  public void index() {
    // 设置页面标题
    setTitle("Shared Cache Manager");
  }

  /**
   * 概览页面请求处理方法，由SCMWebApp初始化时引用路由
   */
  @SuppressWarnings("unused")
  public void overview() {
    // 渲染共享缓存管理器概览页面
    render(SCMOverviewPage.class);
  }
}