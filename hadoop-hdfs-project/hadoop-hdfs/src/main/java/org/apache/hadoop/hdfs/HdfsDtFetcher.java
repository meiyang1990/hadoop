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

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtFetcher;
import org.apache.hadoop.security.token.Token;

/**
 * HDFS 委托令牌获取器，实现 DtFetcher 接口，负责从 HDFS 服务获取委托认证令牌
 * 用于安全认证场景下的跨服务身份凭证获取，令牌获取逻辑在运行时动态加载
 */
public class HdfsDtFetcher implements DtFetcher {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsDtFetcher.class);

  private static final String SERVICE_NAME = HdfsConstants.HDFS_URI_SCHEME;

  private static final String FETCH_FAILED = "Fetch of delegation token failed";

  /**
   * 获取 HDFS 服务名称，用作令牌的服务标识，同时也是 HDFS URI 前缀
   * @return HDFS 服务名称文本
   */
  public Text getServiceName() {
    return new Text(SERVICE_NAME);
  }

  /**
   * 判断当前环境是否需要获取委托令牌
   * @return 安全认证开启时返回true，否则返回false
   */
  public boolean isTokenRequired() {
    return UserGroupInformation.isSecurityEnabled();
  }

  /**
   * 从指定 HDFS 服务获取委托令牌，并添加到凭证对象中
   * @param conf Hadoop 配置对象，用于获取文件系统实例
   * @param creds 凭证对象，用于存储获取到的委托令牌
   * @param renewer 令牌更新者标识，用于请求令牌时指定
   * @param url 目标 HDFS 服务地址
   * @return 获取到的委托令牌，获取失败抛出异常
   * @throws Exception 获取令牌过程中发生的IO或其他异常
   */
  public Token<?> addDelegationTokens(Configuration conf, Credentials creds,
                                  String renewer, String url) throws Exception {
    // 如果URL未带HDFS前缀，自动补全
    if (!url.startsWith(getServiceName().toString())) {
      url = getServiceName().toString() + "://" + url;
    }
    // 获取对应HDFS文件系统实例
    FileSystem fs = FileSystem.get(URI.create(url), conf);
    // 从文件系统获取委托令牌
    Token<?> token = fs.getDelegationToken(renewer);
    // 令牌为空则记录日志并抛出异常
    if (token == null) {
      LOG.error(FETCH_FAILED);
      throw new IOException(FETCH_FAILED);
    }
    // 将获取到的令牌添加到凭证对象
    creds.addToken(token.getService(), token);
    return token;
  }
}