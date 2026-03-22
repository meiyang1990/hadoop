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
 * HDFS Web REST API中URI路径参数封装类
 * 用于从REST请求URI中提取并解析HDFS文件系统路径参数
 */
public class UriFsPathParam extends StringParam {
  /** 参数名称 */
  public static final String NAME = "path";

  private static final Domain DOMAIN = new Domain(NAME, null);

  /**
   * 构造方法，从字符串构建路径参数对象
   * @param str 参数值的字符串表示
   */
  public UriFsPathParam(String str) {
    super(DOMAIN, str);
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * 获取拼接完成的绝对路径
   * 因为原始URI中首斜杠会被去掉，此处需要补回
   * @return 完整的绝对路径，如果原参数为空则返回null
   */
  public final String getAbsolutePath() {
    final String path = getValue(); //The first / has been stripped out.
    return path == null? null: "/" + path;
  }
}