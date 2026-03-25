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

package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.yarn.webapp.Controller;
import com.google.inject.Inject;

/**
 * 全局策略生成器(GPG) Web UI 请求控制器，处理不同页面路由请求
 */
public class GPGController extends Controller {

  @Inject
  GPGController(RequestContext ctx) {
    super(ctx);
  }

  @Override
  public void index() {
    setTitle("GPG");
    render(GPGOverviewPage.class);
  }

  public void overview() {
    setTitle("GPG");
    render(GPGOverviewPage.class);
  }

  public void policies() {
    setTitle("Global Policy Generator Policies");
    render(GPGPoliciesPage.class);
  }
}