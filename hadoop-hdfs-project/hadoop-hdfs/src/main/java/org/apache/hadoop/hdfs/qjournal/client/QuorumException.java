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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;

/**
 * 日志写入仲裁调用中，当异常节点数量过多无法达成仲裁时抛出的异常
 * 用于在HDFS QJM（仲裁日志管理器）中，汇总收集多个JN节点响应时异常情况
 */
/**
 * Exception thrown when too many exceptions occur while gathering
 * responses to a quorum call. 
 */
class QuorumException extends IOException {

  /**
   * 构建包含成功响应和异常信息汇总的QuorumException实例
   * 汇总展示哪些节点成功响应，哪些节点抛出异常
   * @param <K> 仲裁调用中节点的键类型
   * @param <V> 成功响应的数据类型
   * @param simpleMsg 异常基础描述信息
   * @param successes 成功响应结果集合
   * @param exceptions 节点抛出的异常集合
   * @return 构造完成的QuorumException实例
   */
  public static <K, V> QuorumException create(
      String simpleMsg,
      Map<K, V> successes,
      Map<K, Throwable> exceptions) {
    Preconditions.checkArgument(!exceptions.isEmpty(),
        "Must pass exceptions");
    
    // 构建异常消息
    StringBuilder msg = new StringBuilder();
    msg.append(simpleMsg).append(". ");
    // 追加成功响应部分
    if (!successes.isEmpty()) {
      msg.append(successes.size()).append(" successful responses:\n");
      
      // 使用Guava Joiner格式化输出成功响应
      Joiner.on("\n")
          .useForNull("null [success]")
          .withKeyValueSeparator(": ")
          .appendTo(msg, successes);
      msg.append("\n");
    }
    
    // 追加异常信息部分
    msg.append(exceptions.size() + " exceptions thrown:\n");
    boolean isFirst = true;
    
    // 遍历所有节点异常，格式化输出
    for (Map.Entry<K, Throwable> e : exceptions.entrySet()) {
      // 多个异常间换行分隔
      if (!isFirst) {
        msg.append("\n");
      }
      isFirst = false;
      
      // 输出节点键
      msg.append(e.getKey()).append(": ");
      
      // 根据异常类型选择不同格式展示
      if (e.getValue() instanceof RuntimeException) {
        msg.append(StringUtils.stringifyException(e.getValue()));
      } else if (e.getValue().getLocalizedMessage() != null) {
        msg.append(e.getValue().getLocalizedMessage());
      } else {
        msg.append(StringUtils.stringifyException(e.getValue()));
      }
    }
    return new QuorumException(msg.toString());
  }

  private QuorumException(String msg) {
    super(msg);
  }

  private static final long serialVersionUID = 1L;
}