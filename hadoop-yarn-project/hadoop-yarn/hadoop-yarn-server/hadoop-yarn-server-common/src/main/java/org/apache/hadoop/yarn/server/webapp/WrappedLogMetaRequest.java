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
package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * 客户端日志元数据请求包装类，封装了YARN Web UI发起的日志元数据查询请求，
 * 内部将请求转换为标准日志聚合请求并调用文件控制器获取结果。
 */
public class WrappedLogMetaRequest {

  private final LogAggregationFileControllerFactory factory;
  private final ApplicationId appId;
  private final String appOwner;
  private final ContainerId containerId;
  private final String nodeId;
  private final ApplicationAttemptId applicationAttemptId;

  private WrappedLogMetaRequest(Builder builder) {
    this.factory = builder.factory;
    this.appId = builder.appId;
    this.appOwner = builder.appOwner;
    this.containerId = builder.containerId;
    this.nodeId = builder.nodeId;
    this.applicationAttemptId = builder.applicationAttemptId;
  }

  /**
   * WrappedLogMetaRequest 的构建器，采用Builder模式构造请求对象。
   */
  public static class Builder {
    private LogAggregationFileControllerFactory factory;
    private ApplicationId appId;
    private String appOwner;
    private ContainerId containerId;
    private String nodeId;
    private ApplicationAttemptId applicationAttemptId;

    Builder() {
    }

    Builder setFactory(LogAggregationFileControllerFactory logFactory) {
      this.factory = logFactory;
      return this;
    }

    public Builder setApplicationId(ApplicationId applicationId) {
      this.appId = applicationId;
      return this;
    }

    Builder setNodeId(String nid) {
      this.nodeId = nid;
      return this;
    }

    /**
     * 从字符串解析并设置容器ID
     * @param containerIdStr 容器ID字符串，可为null
     * @return 当前构建器实例
     */
    public Builder setContainerId(@Nullable String containerIdStr) {
      if (containerIdStr != null) {
        this.containerId = ContainerId.fromString(containerIdStr);
      }
      return this;
    }

    Builder setAppOwner(String user) {
      this.appOwner = user;
      return this;
    }

    public Builder setApplicationAttemptId(ApplicationAttemptId appAttemptId) {
      this.applicationAttemptId = appAttemptId;
      return this;
    }

    String getAppId() {
      return WrappedLogMetaRequest.getAppId(appId, applicationAttemptId,
          containerId);
    }

    /**
     * 构建 WrappedLogMetaRequest 实例，校验必要参数
     * @return 构造完成的请求对象
     */
    WrappedLogMetaRequest build() {
      if (this.factory == null) {
        throw new AssertionError("WrappedLogMetaRequest's builder should be " +
            "given a LogAggregationFileControllerFactory as parameter.");
      }
      return new WrappedLogMetaRequest(this);
    }
  }

  /**
   * 获取构建器实例
   * @return 新的构建器实例
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * 根据已有信息推导应用ID字符串，从容器/应用尝试中获取应用ID
   * @param appId 直接提供的应用ID
   * @param applicationAttemptId 应用尝试ID
   * @param containerId 容器ID
   * @return 应用ID字符串
   */
  private static String getAppId(ApplicationId appId,
      ApplicationAttemptId applicationAttemptId, ContainerId containerId) {
    if (appId == null) {
      if (applicationAttemptId == null) {
        return containerId.getApplicationAttemptId().getApplicationId()
            .toString();
      } else {
        return applicationAttemptId.getApplicationId().toString();
      }
    }
    return appId.toString();
  }

  /**
   * 获取当前请求对应应用ID的字符串形式
   * @return 应用ID字符串
   */
  public String getAppId() {
    return getAppId(appId, applicationAttemptId, containerId);
  }

  /**
   * 获取当前请求对应应用尝试ID的字符串形式
   * @return 应用尝试ID字符串，无信息时返回null
   */
  public String getAppAttemptId() {
    if (applicationAttemptId == null) {
      if (containerId != null) {
        return containerId.getApplicationAttemptId().toString();
      } else {
        return null;
      }
    } else {
      return applicationAttemptId.toString();
    }
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * 构造标准日志聚合请求，调用日志聚合文件控制器读取日志元数据
   *
   * @return 对应应用/应用尝试/容器的日志元数据列表
   * @throws IOException 读取日志元数据时发生IO异常
   */
  public List<ContainerLogMeta> getContainerLogMetas() throws IOException {
    // 从字符串解析应用ID对象
    ApplicationId applicationId = ApplicationId.fromString(getAppId());
    // 构造标准容器日志请求对象
    ContainerLogsRequest request = new ContainerLogsRequest();
    request.setAppId(applicationId);
    request.setAppAttemptId(applicationAttemptId);
    if (containerId != null) {
      request.setContainerId(containerId.toString());
    }
    request.setAppOwner(appOwner);
    request.setNodeId(nodeId);
    // 获取对应文件控制器，读取聚合日志元数据并返回
    return factory.getFileControllerForRead(applicationId, appOwner)
        .readAggregatedLogsMeta(request);
  }
}