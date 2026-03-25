// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.Counters.CountersExceededException;

/**
 * 计数器数量超过上限异常
 * 当MapReduce任务中计数器数量超出配置的最大限制时抛出该异常
 */
@InterfaceAudience.Private
public class LimitExceededException extends CountersExceededException {

  private static final long serialVersionUID = 1L;

  /**
   * 构造带错误信息的异常对象
   * @param msg 异常错误信息
   */
  public LimitExceededException(String msg) {
    super(msg);
  }

  /**
   * 链式构造异常对象，仅用于包装关联的同类异常
   * @param cause 原始的LimitExceededException异常
   */
  public LimitExceededException(LimitExceededException cause) {
    super(cause);
  }
}