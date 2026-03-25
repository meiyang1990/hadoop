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

/**
 * 预约配额规划异常，当预约创建/更新操作超过用户配额限制时抛出
 */
@Public
@Unstable
public class PlanningQuotaException extends PlanningException {

  private static final long serialVersionUID = 8206629288380246166L;

  /**
   * 使用指定错误信息构造异常
   * @param message 错误描述信息
   */
  public PlanningQuotaException(String message) {
    super(message);
  }

  /**
   * 使用指定根异常构造异常
   * @param cause 根异常
   */
  public PlanningQuotaException(Throwable cause) {
    super(cause);
  }

  /**
   * 使用指定错误信息和根异常构造异常
   * @param message 错误描述信息
   * @param cause 根异常
   */
  public PlanningQuotaException(String message, Throwable cause) {
    super(message, cause);
  }

}