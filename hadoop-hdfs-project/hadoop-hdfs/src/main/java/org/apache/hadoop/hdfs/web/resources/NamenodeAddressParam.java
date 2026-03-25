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

import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * HDFS Web REST API 参数类，封装NameNode RPC地址参数。
 * 用于在REST接口中传递目标NameNode的RPC地址信息，支持联邦集群场景。
 */
public class NamenodeAddressParam extends StringParam {
  /** 参数名称，在URL查询参数中使用。 */
  public static final String NAME = "namenoderpcaddress";
  /** 参数默认值，为空字符串表示未指定。 */
  public static final String DEFAULT = "";

  /** 参数定义域，定义参数的名称和校验规则。 */
  private static final Domain DOMAIN = new Domain(NAME, null);

  /**
   * 构造方法，通过字符串值创建NameNode RPC地址参数。
   * @param str 参数值的字符串表示
   */
  public NamenodeAddressParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: DOMAIN.parse(str));
  }

  /**
   * 构造方法，通过已有的NameNode对象提取RPC地址创建参数。
   * @param namenode NameNode服务对象，从中获取RPC服务地址
   */
  public NamenodeAddressParam(final NameNode namenode) {
    super(DOMAIN, namenode.getTokenServiceName());
  }

  @Override
  public String getName() {
    return NAME;
  }
}