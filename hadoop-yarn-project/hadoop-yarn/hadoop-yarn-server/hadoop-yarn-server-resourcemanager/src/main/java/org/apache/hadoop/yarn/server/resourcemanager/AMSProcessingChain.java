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
<<<<<<< HEAD
 * ApplicationMaster 服务处理链
 * 维护一个 ApplicationMasterServiceProcessor 处理器链，支持动态添加处理器到链首，
 * 通过责任链模式依次处理 AM 的注册、资源分配和完成等请求
=======
 * 维护 ApplicationMasterService 处理器链，支持对AM请求的链式处理。
 * 实现了 ApplicationMasterServiceProcessor 接口，统一对外提供处理入口。
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
 */
class AMSProcessingChain implements ApplicationMasterServiceProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AMSProcessingChain.class);

<<<<<<< HEAD
  // 处理链的头节点，所有请求从这里开始处理
=======
  // 处理器链的头节点
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
  private ApplicationMasterServiceProcessor head;
  // ResourceManager上下文对象
  private RMContext rmContext;

  /**
<<<<<<< HEAD
   * 构造方法：必须提供至少一个根处理器来初始化处理链
   * @param rootProcessor 处理链的根处理器
=======
   * 构造处理器链，必须指定根处理器（至少一个处理器）。
   * @param rootProcessor 根处理器，是链的起始节点
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
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
    // 保存RM上下文对象
    this.rmContext = (RMContext)amsContext;
<<<<<<< HEAD
    // 根处理器的 next 为 null，表示链的末端
=======
    // 初始化头节点，头节点后继处理器为空
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
    this.head.init(amsContext, null);
  }

  /**
<<<<<<< HEAD
   * 动态添加处理器到链首（支持运行时扩展）
   * 新处理器的 next 指向原 head，实现插入操作
=======
   * 将新处理器添加到处理器链头部，成为新的入口节点。
   * @param processor 要添加的处理器
>>>>>>> a7f26154e2430da367a92d826f772851319cf52d
   */
  public synchronized void addProcessor(
      ApplicationMasterServiceProcessor processor) {
    LOG.info("Adding [" + processor.getClass().getName() + "] tp top of" +
        " AMS Processing chain. ");
    // 新处理器的后继设置为当前头节点
    processor.init(this.rmContext, this.head);
    // 更新头节点为新处理器
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
    // 从链头开始处理AM注册请求
    this.head.registerApplicationMaster(applicationAttemptId, request, resp);
  }

  /**
   * 资源分配请求：委托给处理链头部处理
   */
  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // 从链头开始处理AM资源分配请求
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
    // 从链头开始处理AM完成请求
    this.head.finishApplicationMaster(applicationAttemptId, request, response);
  }
}