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

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 表示YARN资源管理器(RM)发生不可恢复错误的致命事件，用于触发RM的异常退出流程。
 */
public class RMFatalEvent extends AbstractEvent<RMFatalEventType> {
  // 触发该致命事件的异常原因
  private final Exception cause;
  // 错误描述信息
  private final String message;

  /**
   * 构造仅包含错误信息的RM致命事件。
   * @param rmFatalEventType 致命事件类型
   * @param message 错误原因描述
   */
  public RMFatalEvent(RMFatalEventType rmFatalEventType, String message) {
    this(rmFatalEventType, null, message);
  }

  /**
   * 构造包含异常原因的RM致命事件。
   * @param rmFatalEventType 致命事件类型
   * @param cause 触发事件的源异常
   */
  public RMFatalEvent(RMFatalEventType rmFatalEventType, Exception cause) {
    this(rmFatalEventType, cause, null);
  }

  /**
   * 构造完整包含事件类型、异常原因和描述信息的RM致命事件。
   * @param rmFatalEventType 致命事件类型
   * @param cause 触发事件的源异常
   * @param message 错误原因描述
   */
  public RMFatalEvent(RMFatalEventType rmFatalEventType, Exception cause,
      String message) {
    super(rmFatalEventType);
    this.cause = cause;
    this.message = message;
  }

  /**
   * 获取完整的错误说明，包含描述信息和异常栈信息。
   * @return 格式化后的完整错误说明字符串
   */
  public String getExplanation() {
    StringBuilder sb = new StringBuilder();

    if (message != null) {
      sb.append(message);

      if (cause != null) {
        sb.append(": ");
      }
    }

    if (cause != null) {
      sb.append(StringUtils.stringifyException(cause));
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    return String.format("RMFatalEvent of type %s, caused by %s",
        getType().name(), getExplanation());
  }
}