// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp.dao;

import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN Router联邦子集群查询结果封装类，用于包装对子集群请求的返回结果、子集群信息和异常信息。
 * @param <R> 泛型，请求响应结果的类型
 */
public class SubClusterResult<R> {
  private SubClusterInfo subClusterInfo;
  private R response;
  private Exception exception;

  public SubClusterResult() {
  }

  /**
   * 构造一个子集群查询结果对象。
   * @param subCluster 子集群基础信息
   * @param res 子集群请求响应结果
   * @param ex 请求过程中发生的异常，无异常则为null
   */
  public SubClusterResult(SubClusterInfo subCluster, R res, Exception ex) {
    this.subClusterInfo = subCluster;
    this.response = res;
    this.exception = ex;
  }

  public SubClusterInfo getSubClusterInfo() {
    return subClusterInfo;
  }

  public void setSubClusterInfo(SubClusterInfo subClusterInfo) {
    this.subClusterInfo = subClusterInfo;
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }

  public R getResponse() {
    return response;
  }

  public void setResponse(R response) {
    this.response = response;
  }
}