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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.webapp.NotFoundException;

/**
 * 从HBase存储读取时间线数据的抽象基类，提供对读取上下文校验和补全的基础能力。
 */
public abstract class AbstractTimelineStorageReader {

  private final TimelineReaderContext context;
  /**
   * 用于查询流上下文关联信息的应用-流关联表读写器。
   */
  private final AppToFlowTableRW appToFlowTable = new AppToFlowTableRW();

  public AbstractTimelineStorageReader(TimelineReaderContext ctxt) {
    context = ctxt;
  }

  protected TimelineReaderContext getContext() {
    return context;
  }

  /**
   * 从AppToFlow表查询指定应用对应的流上下文信息。
   *
   * @param appToFlowRowKey 应用-流关联行键，标识集群和应用ID
   * @param clusterId 集群ID
   * @param hbaseConf HBase配置
   * @param conn HBase连接
   * @return 查询到的流上下文信息
   * @throws IOException 查询过程中发生IO异常
   */
  protected FlowContext lookupFlowContext(AppToFlowRowKey appToFlowRowKey,
      String clusterId, Configuration hbaseConf, Connection conn)
      throws IOException {
    // 生成行键字节数组
    byte[] rowKey = appToFlowRowKey.getRowKey();
    // 创建HBase Get查询请求
    Get get = new Get(rowKey);
    // 执行查询获取结果
    Result result = appToFlowTable.getResult(hbaseConf, conn, get);
    if (result != null && !result.isEmpty()) {
      // 从查询结果中读取流名称
      Object flowName = ColumnRWHelper.readResult(
          result, AppToFlowColumnPrefix.FLOW_NAME, clusterId);
      // 从查询结果中读取流运行ID
      Object flowRunId = ColumnRWHelper.readResult(
          result, AppToFlowColumnPrefix.FLOW_RUN_ID, clusterId);
      // 从查询结果中读取用户ID
      Object userId = ColumnRWHelper.readResult(
          result, AppToFlowColumnPrefix.USER_ID, clusterId);
      if (flowName == null || userId == null || flowRunId == null) {
        // 缺失必要字段，抛出找不到异常
        throw new NotFoundException(
            "Unable to find the context flow name, and flow run id, "
            + "and user id for clusterId=" + clusterId
            + ", appId=" + appToFlowRowKey.getAppId());
      }
      // 封装结果返回
      return new FlowContext((String)userId, (String)flowName,
          ((Number)flowRunId).longValue());
    } else {
      // 查询不到记录，抛出找不到异常
      throw new NotFoundException(
          "Unable to find the context flow name, and flow run id, "
          + "and user id for clusterId=" + clusterId
          + ", appId=" + appToFlowRowKey.getAppId());
    }
  }

  /**
   * 补全读取上下文中未提供的参数，设置默认值。
   *
   * @param hbaseConf HBase配置
   * @param conn HBase连接
   * @throws IOException 补全参数过程中发生异常
   */
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    defaultAugmentParams(hbaseConf, conn);
  }

  /**
   * 默认参数补全实现，供所有时间线读取器使用。
   *
   * @param hbaseConf HBase配置
   * @param conn HBase连接
   * @throws IOException 补全参数过程中发生异常
   */
  final protected void defaultAugmentParams(Configuration hbaseConf,
      Connection conn) throws IOException {
    // 如果流名称、流运行ID、用户ID任意一个未提供，则从AppToFlow表查询补全
    if (context.getFlowName() == null || context.getFlowRunId() == null
        || context.getUserId() == null) {
      // 根据应用ID构造应用-流关联行键
      AppToFlowRowKey appToFlowRowKey =
          new AppToFlowRowKey(context.getAppId());
      // 查询流上下文信息
      FlowContext flowContext =
          lookupFlowContext(appToFlowRowKey, context.getClusterId(), hbaseConf,
              conn);
      // 将查询到的信息设置到上下文中
      context.setFlowName(flowContext.flowName);
      context.setFlowRunId(flowContext.flowRunId);
      context.setUserId(flowContext.userId);
    }
  }

  /**
   * 校验读取实体所需的必要参数，由子类实现具体校验逻辑。
   */
  protected abstract void validateParams();

  /**
   * 封装流上下文信息的不可变数据类。
   */
  protected static class FlowContext {
    private final String userId;
    private final String flowName;
    private final Long flowRunId;

    public FlowContext(String user, String flowName, Long flowRunId) {
      this.userId = user;
      this.flowName = flowName;
      this.flowRunId = flowRunId;
    }

    protected String getUserId() {
      return userId;
    }

    protected String getFlowName() {
      return flowName;
    }

    protected Long getFlowRunId() {
      return flowRunId;
    }
  }
}