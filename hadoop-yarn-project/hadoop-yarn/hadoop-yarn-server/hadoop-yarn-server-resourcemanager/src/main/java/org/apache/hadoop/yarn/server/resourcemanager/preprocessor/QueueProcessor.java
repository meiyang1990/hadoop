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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

/**
 * 应用提交队列预处理器，将指定队列名称设置到应用提交上下文中
 * 属于YARN应用提交前置处理链的一部分，用于覆盖应用提交时指定的队列
 */
class QueueProcessor implements ContextProcessor {
  @Override
  public void process(String host, String value, ApplicationId applicationId,
      ApplicationSubmissionContext submissionContext) {
    // 将处理得到的队列名称设置到应用提交上下文
    submissionContext.setQueue(value);
  }
}