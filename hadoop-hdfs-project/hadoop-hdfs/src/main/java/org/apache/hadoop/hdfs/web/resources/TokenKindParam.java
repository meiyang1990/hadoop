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

package org.apache.hadoop.hdfs.web.resources;

/**
 * HDFS Web API 令牌类型参数封装类，用于解析和封装REST请求中的令牌类型查询参数。
 * 负责验证参数格式，为Web身份验证流程提供参数解析能力。
 */
public class TokenKindParam extends StringParam {

  /** Parameter name */
  public static final String NAME = "kind";
  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  /** 参数校验域，定义令牌类型参数的取值范围规则 */
  private static final StringParam.Domain DOMAIN = new StringParam.Domain(NAME, null);

  /**
   * 构造令牌类型参数对象。
   * @param str 参数值的字符串表示
   */
  public TokenKindParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  /**
   * 获取参数名称。
   * @return 参数名称 "kind"
   */
  @Override
  public String getName() {
    return NAME;
  }
}