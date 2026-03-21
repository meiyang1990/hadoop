// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 资源处理器责任链工具类，将请求按顺序转发给链中所有资源处理器处理，聚合所有处理器的操作结果
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerChain implements ResourceHandler {
  // 存储责任链中的所有资源处理器
  private final List<ResourceHandler> resourceHandlers;

  /**
   * 构造资源处理器责任链
   * @param resourceHandlers 责任链中的资源处理器列表
   */
  public ResourceHandlerChain(List<ResourceHandler> resourceHandlers) {
    this.resourceHandlers = resourceHandlers;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    // 聚合所有处理器返回的特权操作
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    // 遍历调用所有处理器的bootstrap方法
    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.bootstrap(configuration);
      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    // 聚合所有处理器返回的特权操作
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    // 遍历调用所有处理器的preStart方法
    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.preStart(container);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    // 聚合所有处理器返回的特权操作
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    // 遍历调用所有处理器的reacquireContainer方法
    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.reacquireContainer(containerId);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    // 聚合所有处理器返回的特权操作
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    // 遍历调用所有处理器的updateContainer方法
    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.updateContainer(container);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    // 聚合所有处理器返回的特权操作
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    // 遍历调用所有处理器的postComplete方法
    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.postComplete(containerId);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    // 聚合所有处理器返回的特权操作
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    // 遍历调用所有处理器的teardown方法
    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.teardown();

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  /**
   * 获取不可修改的资源处理器列表，仅用于测试
   * @return 资源处理器只读列表
   */
  @VisibleForTesting
  public List<ResourceHandler> getResourceHandlerList() {
    return Collections.unmodifiableList(resourceHandlers);
  }

  @Override
  public String toString() {
    return ResourceHandlerChain.class.getName() + "{" +
        "resourceHandlers=" + resourceHandlers +
        '}';
  }
}