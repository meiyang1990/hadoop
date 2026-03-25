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
 * HDFS Web API 获取Delegation Token请求的service参数封装类.
 * 用于在REST接口中解析和承载请求参数中的目标服务标识，支持跨服务获取token场景
 */
public class TokenServiceParam extends StringParam {

  /** Parameter name */
  public static final String NAME = "service";
  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  private static final StringParam.Domain DOMAIN = new StringParam.Domain(NAME, null);

  /**
   * 构造方法，根据输入字符串构造service参数对象.
   * @param str 参数值字符串
   */
  public TokenServiceParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  @Override
  public String getName() {
    return NAME;
  }
}