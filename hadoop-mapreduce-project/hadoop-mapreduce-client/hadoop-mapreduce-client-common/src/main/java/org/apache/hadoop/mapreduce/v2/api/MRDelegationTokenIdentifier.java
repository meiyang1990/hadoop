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

package org.apache.hadoop.mapreduce.v2.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 * MapReduce 代理令牌标识符实现类，用于标识JobHistoryServer颁发的代理令牌，
 * 允许MR任务通过代理令牌认证访问JobHistoryServer服务。
 * 继承了Hadoop通用代理令牌标识符的基础能力，定义了MR场景下的特定令牌类型。
 */
@Private
// TODO Move to a different package.
public class MRDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {

  // MR代理令牌的类型名称常量
  public static final Text KIND_NAME = new Text("MR_DELEGATION_TOKEN");

 
  /**
   * 空构造函数，用于反序列化创建对象
   */
  public MRDelegationTokenIdentifier() {
  }
  
  /**
   * 创建新的MR代理令牌标识符
   * @param owner 令牌所有者的有效用户名
   * @param renewer 有权更新令牌的用户名
   * @param realUser 令牌对应用户的真实用户名（代理场景下区分有效用户和真实用户）
   */
  public MRDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

 
  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  /**
   * MR代理令牌更新器实现，Hadoop令牌系统自动调用识别对应类型令牌进行更新
   */
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}