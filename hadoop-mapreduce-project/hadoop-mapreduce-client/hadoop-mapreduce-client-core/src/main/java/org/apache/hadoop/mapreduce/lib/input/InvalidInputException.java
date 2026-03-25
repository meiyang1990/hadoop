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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * MapReduce输入数据非法异常，用于批量收集所有输入错误后统一抛出，
 * 让用户可以一次性获取所有输入问题，而不需要逐个发现修复。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidInputException extends IOException {
  private static final long serialVersionUID = -380668190578456802L;
  private List<IOException> problems;
  
  /**
   * 基于收集到的输入错误列表构造异常对象，使用列表第一个异常作为根异常。
   * @param probs 收集到的输入问题列表，不进行拷贝直接持有
   */
  public InvalidInputException(List<IOException> probs) {
    problems = probs;
    if (!probs.isEmpty()) {
      initCause(probs.get(0));
    }
  }
  
  /**
   * 获取所有收集到的输入问题列表。
   * @return 输入问题列表，禁止修改
   */
  public List<IOException> getProblems() {
    return problems;
  }
  
  /**
   * 拼接所有输入问题的错误信息，生成汇总异常信息。
   * @return 拼接所有异常信息后的汇总字符串
   */
  public String getMessage() {
    StringBuilder result = new StringBuilder();
    Iterator<IOException> itr = problems.iterator();
    while(itr.hasNext()) {
      result.append(itr.next().getMessage());
      if (itr.hasNext()) {
        result.append("\n");
      }
    }
    return result.toString();
  }
}