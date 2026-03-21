// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.slf4j.Logger;

/**
 * 公平调度器配置转容量调度器配置的转换选项容器，
 * 封装干运行模式、规则检查、调度器类型等配置，并提供各类错误和警告的统一处理逻辑。
 */
public class ConversionOptions {
  private DryRunResultHolder dryRunResultHolder;
  private boolean dryRun;
  private boolean noTerminalRuleCheck;
  private boolean enableAsyncScheduler;

  /**
   * 构造转换选项实例，初始化干运行结果持有器和干运行模式。
   * @param dryRunResultHolder 干运行结果持有器
   * @param dryRun 是否开启干运行模式（仅检查不实际写入配置）
   */
  public ConversionOptions(DryRunResultHolder dryRunResultHolder,
      boolean dryRun) {
    this.dryRunResultHolder = dryRunResultHolder;
    this.dryRun = dryRun;
  }

  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  /**
   * 设置是否禁用终端规则检查。
   * @param ruleTerminalCheck 是否禁用终端规则检查
   */
  public void setNoTerminalRuleCheck(boolean ruleTerminalCheck) {
    this.noTerminalRuleCheck = ruleTerminalCheck;
  }

  /**
   * 获取是否禁用终端规则检查。
   * @return true表示禁用检查，false表示启用检查
   */
  public boolean isNoRuleTerminalCheck() {
    return noTerminalRuleCheck;
  }

  /**
   * 设置是否启用异步容量调度器。
   * @param enableAsyncScheduler 是否启用异步调度器
   */
  public void setEnableAsyncScheduler(boolean enableAsyncScheduler) {
    this.enableAsyncScheduler = enableAsyncScheduler;
  }

  /**
   * 获取是否启用异步容量调度器。
   * @return true表示启用异步调度器，false表示使用同步调度器
   */
  public boolean isEnableAsyncScheduler() {
    return enableAsyncScheduler;
  }

  /**
   * 处理转换警告信息，干运行模式收集到结果中，否则输出日志。
   * @param msg 警告信息
   * @param log 日志对象
   */
  public void handleWarning(String msg, Logger log) {
    if (dryRun) {
      dryRunResultHolder.addDryRunWarning(msg);
    } else {
      log.warn(msg);
    }
  }

  /**
   * 处理不支持的配置属性错误，干运行模式收集到结果中，否则抛出异常。
   * @param msg 错误信息
   */
  public void handleError(String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      throw new UnsupportedPropertyException(msg);
    }
  }

  /**
   * 处理转换过程错误，干运行模式收集到结果中，否则抛出异常。
   * @param msg 错误信息
   */
  public void handleConversionError(String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      throw new ConversionException(msg);
    }
  }

  /**
   * 处理前置条件检查错误，干运行模式收集到结果中，否则抛出异常。
   * @param msg 错误信息
   */
  public void handlePreconditionError(String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      throw new PreconditionException(msg);
    }
  }

  /**
   * 处理配置验证失败，日志记录错误，干运行模式标记验证失败。
   * @param e 异常对象
   * @param msg 错误信息
   */
  public void handleVerificationFailure(Throwable e, String msg) {
    FSConfigToCSConfigArgumentHandler.logAndStdErr(e, msg);
    if (dryRun) {
      dryRunResultHolder.setVerificationFailed();
    }
  }

  /**
   * 解析完成后的处理，干运行模式打印收集到的结果。
   */
  public void handleParsingFinished() {
    if (dryRun) {
      dryRunResultHolder.printDryRunResults();
    }
  }

  /**
   * 处理通用异常，干运行模式收集错误，否则日志记录错误。
   * @param e 异常对象
   * @param msg 错误信息
   */
  public void handleGenericException(Exception e, String msg) {
    if (dryRun) {
      dryRunResultHolder.addDryRunError(msg);
    } else {
      FSConfigToCSConfigArgumentHandler.logAndStdErr(e, msg);
    }
  }
}