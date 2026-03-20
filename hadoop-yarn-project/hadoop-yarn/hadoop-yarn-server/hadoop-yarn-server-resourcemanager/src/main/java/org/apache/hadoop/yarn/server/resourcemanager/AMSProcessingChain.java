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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ApplicationMaster 服务处理链
 * 维护一个 ApplicationMasterServiceProcessor 处理器链，支持动态添加处理器到链首，
 * 通过责任链模式依次处理 AM 的注册、资源分配和完成等请求
 */
class AMSProcessingChain implements ApplicationMasterServiceProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AMSProcessingChain.class);

  // 处理链的头节点，所有请求从这里开始处理
  private ApplicationMasterServiceProcessor head;
  private RMContext rmContext;

  /**
   * 构造方法：必须提供至少一个根处理器来初始化处理链
   * @param rootProcessor 处理链的根处理器
   */
  AMSProcessingChain(ApplicationMasterServiceProcessor rootProcessor) {
    if (rootProcessor == null) {
      throw new YarnRuntimeException("No root ApplicationMasterService" +
          "Processor specified for the processing chain..");
    }
    this.head = rootProcessor;
  }

  /**
   * 初始化处理链：将根处理器的 next 设为 null（作为链尾）
   */
  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    LOG.info("Initializing AMS Processing chain. Root Processor=["
        + this.head.getClass().getName() + "].");
    this.rmContext = (RMContext)amsContext;
    // 根处理器的 next 为 null，表示链的末端
    this.head.init(amsContext, null);
  }

  /**
   * 动态添加处理器到链首（支持运行时扩展）
   * 新处理器的 next 指向原 head，实现插入操作
   */
  public synchronized void addProcessor(
      ApplicationMasterServiceProcessor processor) {
    LOG.info("Adding [" + processor.getClass().getName() + "] tp top of" +
        " AMS Processing chain. ");
    processor.init(this.rmContext, this.head);
    this.head = processor;
  }

  /**
   * AM 注册请求：委托给处理链头部处理
   */
  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse resp) throws IOException, YarnException {
    this.head.registerApplicationMaster(applicationAttemptId, request, resp);
  }

  /**
   * 资源分配请求：委托给处理链头部处理
   */
  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    this.head.allocate(appAttemptId, request, response);
  }

  /**
   * AM 完成通知：委托给处理链头部处理
   */
  @Override
  public void finishApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    this.head.finishApplicationMaster(applicationAttemptId, request, response);
  }
}
