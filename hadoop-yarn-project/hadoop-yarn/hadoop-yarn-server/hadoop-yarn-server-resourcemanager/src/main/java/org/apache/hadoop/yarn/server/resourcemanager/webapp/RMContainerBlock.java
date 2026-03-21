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

import java.io.IOException;

import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.webapp.ContainerBlock;

import com.google.inject.Inject;

/**
 * ResourceManager Web UI 容器信息页面块，负责渲染容器详情页面
 * 继承通用ContainerBlock，实现RM侧的容器报告获取逻辑
 */
public class RMContainerBlock extends ContainerBlock {

  private final ResourceManager rm;

  /**
   * 构造方法，通过Guice注入ResourceManager实例
   * @param resourceManager ResourceManager核心实例
   * @param ctx Web视图上下文
   */
  @Inject
  public RMContainerBlock(ResourceManager resourceManager, ViewContext ctx) {
    super(null, ctx);
    this.rm = resourceManager;
  }

  /**
   * 重写获取容器报告的方法，从RM本地客户端服务获取容器信息
   * @param request 获取容器报告请求
   * @return 容器详细报告
   * @throws YarnException Yarn异常
   * @throws IOException IO异常
   */
  @Override
  protected ContainerReport getContainerReport(
      final GetContainerReportRequest request)
      throws YarnException, IOException {
    return rm.getClientRMService().getContainerReport(request)
        .getContainerReport();
  }
}