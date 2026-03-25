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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN RM Web API 凭据信息数据访问对象，用于序列化/反序列化应用凭据信息给前端展示
 */
@XmlRootElement(name = "credentials-info")
@XmlAccessorType(XmlAccessType.FIELD)
public class CredentialsInfo {

  // 令牌列表，key为令牌名称，value为令牌标识
  @XmlElementWrapper(name = "tokens")
  HashMap<String, String> tokens;

  // 密钥列表，key为密钥名称，value为密钥内容
  @XmlElementWrapper(name = "secrets")
  HashMap<String, String> secrets;

  /**
   * 默认构造函数，初始化空的令牌和密钥容器
   */
  public CredentialsInfo() {
    tokens = new HashMap<String, String>();
    secrets = new HashMap<String, String>();
  }

  /**
   * 获取令牌列表
   * @return 令牌键值对集合
   */
  public HashMap<String, String> getTokens() {
    return tokens;
  }

  /**
   * 获取密钥列表
   * @return 密钥键值对集合
   */
  public HashMap<String, String> getSecrets() {
    return secrets;
  }

  /**
   * 设置令牌列表
   * @param tokens 令牌键值对集合
   */
  public void setTokens(HashMap<String, String> tokens) {
    this.tokens = tokens;
  }

  /**
   * 设置密钥列表
   * @param secrets 密钥键值对集合
   */
  public void setSecrets(HashMap<String, String> secrets) {
    this.secrets = secrets;
  }

}