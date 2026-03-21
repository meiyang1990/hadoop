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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * 文件说明：YARN ResourceManager 端代理证书颁发机构(CA)管理器
 * 
 * 核心职责：管理RM Proxy使用的根CA证书和私钥，支持RM重启后的状态恢复，
 *          为ApplicationMaster颁发HTTPS证书，保障AM与RM Proxy之间的HTTPS通信安全
 */
@Private
@InterfaceStability.Unstable
public class ProxyCAManager extends AbstractService implements Recoverable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ProxyCAManager.class);

  /** 代理CA实例，负责实际证书签发 */
  private ProxyCA proxyCA;
  /** ResourceManager上下文，用于获取状态存储等核心组件 */
  private RMContext rmContext;
  /** 标记是否已从之前的RM状态恢复CA信息 */
  private boolean wasRecovered;

  /**
   * 构造ProxyCAManager实例
   * @param proxyCA 代理CA实例
   * @param rmContext RM上下文对象
   */
  public ProxyCAManager(ProxyCA proxyCA, RMContext rmContext) {
    super(ProxyCAManager.class.getName());
    this.proxyCA = proxyCA;
    this.rmContext = rmContext;
    wasRecovered = false;
  }

  @Override
  protected void serviceStart() throws Exception {
    // 未恢复过则初始化新的CA
    if (!wasRecovered) {
      proxyCA.init();
    }
    // 重置恢复标记，下次启动重新判断
    wasRecovered = false;
    // 将当前CA证书和私钥存储到RM状态存储中，供重启恢复使用
    rmContext.getStateStore().storeProxyCACert(
        proxyCA.getCaCert(), proxyCA.getCaKeyPair().getPrivate());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  /** 获取当前代理CA实例 */
  public ProxyCA getProxyCA() {
    return proxyCA;
  }

  /**
   * 从RM恢复状态中恢复CA证书和私钥
   * @param state 已保存的RM状态
   * @throws GeneralSecurityException 安全相关异常
   * @throws IOException IO异常
   */
  public void recover(RMState state)
      throws GeneralSecurityException, IOException {
    LOG.info("Recovering CA Certificate and Private Key");
    // 从恢复状态中读取CA证书
    X509Certificate caCert = state.getProxyCAState().getCaCert();
    // 从恢复状态中读取CA私钥
    PrivateKey caPrivateKey = state.getProxyCAState().getCaPrivateKey();
    // 使用恢复的证书和私钥初始化CA
    proxyCA.init(caCert, caPrivateKey);
    // 标记已完成恢复，启动时不需要重新生成CA
    wasRecovered = true;
  }
}