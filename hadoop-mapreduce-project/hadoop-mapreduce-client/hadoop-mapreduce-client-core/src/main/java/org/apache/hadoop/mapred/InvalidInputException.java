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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 输入数据合法性检查异常，聚合所有输入错误一次性返回给用户，
 * 避免用户逐个发现修复问题，提升错误排查效率。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidInputException extends IOException {
 
  private static final long serialVersionUID = 1L;
  // 存储所有输入错误异常列表
  private List<IOException> problems;
  
  /**
   * 基于输入错误列表构造异常对象
   * 使用列表中第一个异常作为根异常初始化异常链
   * @param probs 需要报告的输入错误列表，不会拷贝该列表直接持有引用
   */
  public InvalidInputException(List<IOException> probs) {
    problems = probs;
    if (!probs.isEmpty()) {
      initCause(probs.get(0));
    }
  }
  
  /**
   * 获取所有输入错误的完整列表
   * @return 输入错误列表，不允许修改
   */
  public List<IOException> getProblems() {
    return problems;
  }
  
  /**
   * 聚合所有输入错误的消息，生成汇总异常信息
   * @return 所有异常消息拼接后的汇总字符串
   */
  @Override
  public String getMessage() {
    StringBuilder result = new StringBuilder();
    Iterator<IOException> itr = problems.iterator();
    // 遍历所有错误异常拼接消息
    while(itr.hasNext()) {
      result.append(itr.next().getMessage());
      // 非最后一个错误添加换行分隔
      if (itr.hasNext()) {
        result.append("\n");
      }
    }
    return result.toString();
  }
}