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

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * RM关键线程未捕获异常处理器，当RM关键线程抛出未捕获异常时，
 * 将触发RM关闭或自动切换到Standby状态，保障集群高可用。
 * 需要在关键线程创建后或入口点调用 {@code setUncaughtExceptionHandler} 安装使用。
 */
@Private
public class RMCriticalThreadUncaughtExceptionHandler
    implements UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      RMCriticalThreadUncaughtExceptionHandler.class);
  private final RMContext rmContext;

  /**
   * 构造异常处理器，持有RM上下文引用
   * @param rmContext ResourceManager上下文对象
   */
  public RMCriticalThreadUncaughtExceptionHandler(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    Exception ex;

    // 将非Exception类型的Throwable包装为YarnException
    if (e instanceof Exception) {
      ex = (Exception)e;
    } else {
      ex = new YarnException(e);
    }

    // 构造关键线程崩溃的致命事件，包含异常信息和线程名称描述
    RMFatalEvent event =
        new RMFatalEvent(RMFatalEventType.CRITICAL_THREAD_CRASH, ex,
            String.format("a critical thread, %s, that exited unexpectedly",
                t.getName()));

    // 将致命事件分发到RM事件处理器，触发后续处理流程
    rmContext.getDispatcher().getEventHandler().handle(event);
  }
}