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
 * 文件级注释：SWebHDFS 安全协议的 Delegation Token 获取器，基于 HdfsDtFetcher 基础实现
 *
 * DtFetcher for SWebHdfsFileSystem using the base class HdfsDtFetcher impl.
 * 类级注释：安全WebHDFS(SWebHdfs)的委托令牌(Delegation Token)获取器，用于SWebHDFS认证场景，
 *          继承HdfsDtFetcher基础实现，提供SWebHDFS专属的服务名称标识
 */
public class SWebHdfsDtFetcher extends HdfsDtFetcher {
  // 日志记录器
  private static final Logger LOG =
      LoggerFactory.getLogger(SWebHdfsDtFetcher.class);

  // SWebHDFS服务名称常量，对应SWebHDFS scheme定义
  private static final String SERVICE_NAME = WebHdfsConstants.SWEBHDFS_SCHEME;

  /**
   * 方法级注释：获取当前服务的名称，用于标识SWebHDFS服务
   * @return SWebHDFS服务名称文本
   */
  @Override
  public Text getServiceName() {
    return new Text(SERVICE_NAME);
  }
}