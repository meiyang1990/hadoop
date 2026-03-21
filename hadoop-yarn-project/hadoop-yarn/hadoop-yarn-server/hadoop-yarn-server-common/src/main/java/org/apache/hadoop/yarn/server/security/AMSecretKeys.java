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

package org.apache.hadoop.yarn.server.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

/**
 * YARN应用 Master (AM) 安全密钥相关常量定义，保存密钥库、信任库相关密钥名称常量。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class AMSecretKeys {

  /** YARN应用AM密钥库密钥名称 */
  public final static Text YARN_APPLICATION_AM_KEYSTORE =
      new Text("yarn.application.am.keystore");
  /** YARN应用AM密钥库密码密钥名称 */
  public final static Text YARN_APPLICATION_AM_KEYSTORE_PASSWORD =
      new Text("yarn.application.am.keystore.password");
  /** YARN应用AM信任库密钥名称 */
  public final static Text YARN_APPLICATION_AM_TRUSTSTORE =
      new Text("yarn.application.am.truststore");
  /** YARN应用AM信任库密码密钥名称 */
  public final static Text YARN_APPLICATION_AM_TRUSTSTORE_PASSWORD =
      new Text("yarn.application.am.truststore.password");

  private AMSecretKeys() {
    // 工具类不允许实例化
  }
}