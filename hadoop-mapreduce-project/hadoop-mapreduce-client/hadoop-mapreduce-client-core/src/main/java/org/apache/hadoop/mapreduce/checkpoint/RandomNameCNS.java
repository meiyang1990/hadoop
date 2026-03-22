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
package org.apache.hadoop.mapreduce.checkpoint;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * 随机名称检查点命名服务，负责生成随机名称的检查点
 * 本类实现CheckpointNamingService接口，提供基于随机字符串的检查点命名策略
 */
public class RandomNameCNS implements CheckpointNamingService {

  /**
   * 生成新的随机检查点名称
   * @return 格式为checkpoint_前缀加8位随机字母数字的检查点名称
   */
  @Override
  public String getNewName() {
    return "checkpoint_" + RandomStringUtils.randomAlphanumeric(8);
  }

}