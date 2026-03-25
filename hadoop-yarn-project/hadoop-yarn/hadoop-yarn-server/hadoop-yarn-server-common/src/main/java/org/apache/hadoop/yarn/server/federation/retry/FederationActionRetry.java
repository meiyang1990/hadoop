// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN联邦操作重试模板接口，定义联邦操作执行与重试逻辑。
 * @param <T> 操作返回结果类型
 */
public interface FederationActionRetry<T> {

  Logger LOG = LoggerFactory.getLogger(FederationActionRetry.class);

  /**
   * 执行联邦操作，由实现类提供具体业务逻辑。
   * @param retry 当前已经重试的次数
   * @return 操作执行结果
   * @throws Exception 操作执行异常
   */
  T run(int retry) throws Exception;

  /**
   * 带重试机制执行联邦操作，失败后按配置次数重试。
   * @param retryCount 最大重试次数
   * @param retrySleepTime 重试前等待时间（毫秒）
   * @return 操作执行结果
   * @throws Exception 超过最大重试次数仍失败时抛出原始异常
   */
  default T runWithRetries(int retryCount, long retrySleepTime) throws Exception {
    int retry = 0;
    while (true) {
      try {
        return run(retry);
      } catch (Exception e) {
        LOG.info("Exception while executing an Federation operation.", e);
        if (++retry > retryCount) {
          LOG.info("Maxed out Federation retries. Giving up!");
          throw e;
        }
        LOG.info("Retrying operation on Federation. Retry no. {}", retry);
        Thread.sleep(retrySleepTime);
      }
    }
  }
}