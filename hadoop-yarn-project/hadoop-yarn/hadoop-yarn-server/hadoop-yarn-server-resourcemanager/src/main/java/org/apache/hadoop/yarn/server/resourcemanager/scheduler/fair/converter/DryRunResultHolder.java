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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * 公平调度器配置转换干运行结果持有器，用于收集存储干运行过程中的警告、错误和验证结果
 */
public class DryRunResultHolder {
  private static final Logger LOG =
      LoggerFactory.getLogger(DryRunResultHolder.class);

  // 警告信息集合
  private Set<String> warnings;
  // 错误信息集合
  private Set<String> errors;
  // 验证是否失败标记
  private boolean verificationFailed;

  /**
   * 构造干运行结果持有器，初始化警告和错误集合
   */
  public DryRunResultHolder() {
    this.warnings = new HashSet<>();
    this.errors = new HashSet<>();
  }

  /**
   * 添加干运行警告信息
   * @param message 警告信息文本
   */
  public void addDryRunWarning(String message) {
    warnings.add(message);
  }

  /**
   * 添加干运行错误信息
   * @param message 错误信息文本
   */
  public void addDryRunError(String message) {
    errors.add(message);
  }

  /**
   * 标记验证过程失败
   */
  public void setVerificationFailed() {
    verificationFailed = true;
  }

  /**
   * 获取不可修改的警告信息集合快照
   * @return 不可修改的警告信息集合
   */
  public Set<String> getWarnings() {
    return ImmutableSet.copyOf(warnings);
  }

  /**
   * 获取不可修改的错误信息集合快照
   * @return 不可修改的错误信息集合
   */
  public Set<String> getErrors() {
    return ImmutableSet.copyOf(errors);
  }

  /**
   * 将干运行结果格式化输出到日志中
   */
  public void printDryRunResults() {
    LOG.info("");
    LOG.info("Results of dry run:");
    LOG.info("");

    int noOfErrors = errors.size();
    int noOfWarnings = warnings.size();

    LOG.info("Number of errors: {}", noOfErrors);
    LOG.info("Number of warnings: {}", noOfWarnings);
    LOG.info("Verification result: {}",
        verificationFailed ? "FAILED" : "PASSED");

    if (noOfErrors > 0) {
      LOG.info("");
      LOG.info("List of errors:");
      errors.forEach(s -> LOG.info(s));
    }

    if (noOfWarnings > 0) {
      LOG.info("");
      LOG.info("List of warnings:");
      warnings.forEach(s -> LOG.info(s));
    }
  }
}