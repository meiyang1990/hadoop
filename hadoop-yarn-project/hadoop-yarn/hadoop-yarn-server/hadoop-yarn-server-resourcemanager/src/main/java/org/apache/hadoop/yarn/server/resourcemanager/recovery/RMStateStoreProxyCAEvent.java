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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * 用于持久化存储ProxyCA证书和私钥信息的RM状态存储事件，在YARN HA恢复场景中保存代理CA凭证状态。
 */
public class RMStateStoreProxyCAEvent extends RMStateStoreEvent {
  // CA根证书
  private X509Certificate caCert;
  // CA私钥
  private PrivateKey caPrivateKey;

  public RMStateStoreProxyCAEvent(RMStateStoreEventType type) {
    super(type);
  }

  /**
   * 构造包含CA证书和私钥的状态存储事件。
   * @param caCert CA根证书
   * @param caPrivateKey CA私钥
   * @param type 事件类型
   */
  public RMStateStoreProxyCAEvent(X509Certificate caCert,
      PrivateKey caPrivateKey, RMStateStoreEventType type) {
    this(type);
    this.caCert = caCert;
    this.caPrivateKey = caPrivateKey;
  }

  public X509Certificate getCaCert() {
    return caCert;
  }

  public PrivateKey getCaPrivateKey() {
    return caPrivateKey;
  }
}