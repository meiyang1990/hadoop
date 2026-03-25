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
import java.net.HttpURLConnection;

/**
 * HTTP GET请求失败异常，封装请求失败时的HTTP响应码信息。
 * 在HDFS服务端内部HTTP调用场景中，当GET请求失败时抛出该异常。
 */
@InterfaceAudience.Private
public class HttpGetFailedException extends IOException {
  private static final long serialVersionUID = 1L;
  // HTTP请求失败时的响应状态码
  private final int responseCode;

  /**
   * 构造HTTP GET请求失败异常，从连接对象中获取响应码。
   * @param msg 异常描述信息
   * @param connection 失败的HTTP连接对象
   * @throws IOException 获取响应码时可能抛出IO异常
   */
  public HttpGetFailedException(String msg, HttpURLConnection connection)
      throws IOException {
    super(msg);
    this.responseCode = connection.getResponseCode();
  }

  /**
   * 获取失败请求的HTTP响应状态码。
   * @return HTTP响应状态码
   */
  public int getResponseCode() {
    return responseCode;
  }
}