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

package org.apache.hadoop.yarn.server.resourcemanager.preprocessor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;


/**
 * YARN ResourceManager 应用提交上下文预处理接口，负责在应用提交到调度器前处理提交上下文。
 * 用于在应用正式提交前对上下文信息进行补充、修改或验证，支持扩展自定义预处理逻辑。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ContextProcessor {
  /**
   * 处理应用提交上下文，可根据输入信息补充或修改提交上下文内容。
   * 该方法会在应用提交流程中被调用，允许预处理逻辑注入自定义配置。
   * 
   * @param host  提交应用的客户端主机地址
   * @param value 需要填充到应用提交上下文中的预处理值
   * @param applicationId  当前提交应用的应用ID
   * @param submissionContext  应用提交上下文对象，可直接修改其内容
   */
  void process(String host, String value, ApplicationId applicationId,
      ApplicationSubmissionContext submissionContext);
}