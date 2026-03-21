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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

/**
 * YARN RM Web UI 容器详情页面，负责渲染单个容器的详细信息页面
 */
public class ContainerPage extends RmView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行公共头部预处理逻辑
    commonPreHead(html);

    // 从请求参数获取容器ID
    String containerId = $(YarnWebParams.CONTAINER_ID);
    // 设置页面标题，参数缺失时返回错误提示
    set(TITLE, containerId.isEmpty() ? "Bad request: missing container ID"
        : join("Container ", $(YarnWebParams.CONTAINER_ID)));
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回容器详情内容区块实现类
    return RMContainerBlock.class;
  }

}