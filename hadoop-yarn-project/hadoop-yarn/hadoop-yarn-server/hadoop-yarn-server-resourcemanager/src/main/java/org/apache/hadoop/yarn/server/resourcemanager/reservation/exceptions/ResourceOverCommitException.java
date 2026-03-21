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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;

/**
 * 资源超额异常，表示当前尝试提交的预留请求会超出资源计划{@link Plan}当前可用的物理资源总量
 */
@Public
@Unstable
public class ResourceOverCommitException extends PlanningException {

  private static final long serialVersionUID = 7070699407526521032L;

  /**
   * 构造带错误消息的资源超额异常
   * @param message 错误描述信息
   */
  public ResourceOverCommitException(String message) {
    super(message);
  }

  /**
   * 构造包装原始异常的资源超额异常
   * @param cause 原始异常
   */
  public ResourceOverCommitException(Throwable cause) {
    super(cause);
  }

  /**
   * 构造带错误消息且包装原始异常的资源超额异常
   * @param message 错误描述信息
   * @param cause 原始异常
   */
  public ResourceOverCommitException(String message, Throwable cause) {
    super(message, cause);
  }

}