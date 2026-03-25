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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

/**
 * NodeManager向ResourceManager注销节点的响应封装类，
 * 包含ResourceManager对NodeManager注销请求的处理结果。
 */
public abstract class UnRegisterNodeManagerResponse {
  /**
   * 创建UnRegisterNodeManagerResponse实例工厂方法
   * @return 新的注销响应实例
   */
  public static UnRegisterNodeManagerResponse newInstance() {
    return Records.newRecord(UnRegisterNodeManagerResponse.class);
  }
}