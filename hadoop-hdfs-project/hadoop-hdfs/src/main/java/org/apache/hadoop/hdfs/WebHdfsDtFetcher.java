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

package org.apache.hadoop.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.io.Text;

/**
 * WebHDFS 委托令牌获取器，基于 HdfsDtFetcher 基础实现，为 WebHdfsFileSystem 提供认证令牌获取能力。
 * 属于 HDFS 安全认证模块，用于获取 WebHDFS 访问所需的 delegation token。
 */
public class WebHdfsDtFetcher extends HdfsDtFetcher {
  // 日志记录器
  private static final Logger LOG =
      LoggerFactory.getLogger(WebHdfsDtFetcher.class);

  // WebHDFS 服务名称，对应URL方案常量
  private static final String SERVICE_NAME = WebHdfsConstants.WEBHDFS_SCHEME;

  /**
   * 获取当前服务的名称，用于标识 WebHDFS 服务的令牌类型。
   * @return WebHDFS 服务名称文本对象
   */
  @Override
  public Text getServiceName() {
    return new Text(SERVICE_NAME);
  }
}