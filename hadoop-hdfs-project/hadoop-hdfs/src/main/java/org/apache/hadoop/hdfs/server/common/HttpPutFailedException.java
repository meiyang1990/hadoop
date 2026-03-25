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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;

/**
 * HTTP PUT请求失败异常
 * HDFS服务端内部发起HTTP PUT请求失败时抛出该异常，保存HTTP响应码便于问题定位
 */
@InterfaceAudience.Private
public class HttpPutFailedException extends IOException {
  private static final long serialVersionUID = 1L;
  // HTTP响应状态码
  private final int responseCode;

  /**
   * 构造HTTP PUT失败异常
   * @param msg 异常错误信息
   * @param responseCode HTTP响应状态码
   * @throws IOException IO异常基类
   */
  public HttpPutFailedException(String msg, int responseCode) throws IOException {
    super(msg);
    this.responseCode = responseCode;
  }

  /**
   * 获取HTTP响应状态码
   * @return HTTP响应状态码
   */
  public int getResponseCode() {
    return responseCode;
  }
}