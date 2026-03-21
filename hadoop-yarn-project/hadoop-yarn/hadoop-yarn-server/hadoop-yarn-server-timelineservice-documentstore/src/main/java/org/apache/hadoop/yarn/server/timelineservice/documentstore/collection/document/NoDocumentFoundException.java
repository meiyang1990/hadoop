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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document;

import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;

import java.io.IOException;

/**
 * 文档存储服务中未找到请求文档时抛出的异常，对所有类型的文档存储提供商通用，读取文档不存在时触发。
 */
public class NoDocumentFoundException extends IOException {

  /**
   * 用指定的详细信息构造异常实例。
   * @param message 异常详细信息
   */
  public NoDocumentFoundException(String message) {
    super(message);
  }
}