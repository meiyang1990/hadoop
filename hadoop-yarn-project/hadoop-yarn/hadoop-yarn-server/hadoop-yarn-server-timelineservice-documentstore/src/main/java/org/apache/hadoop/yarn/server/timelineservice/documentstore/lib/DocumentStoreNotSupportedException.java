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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.lib;

// 文档存储不支持异常，表示配置的文档存储供应商不属于支持的DocumentStoreVendor之一
/**
 * Indicates that the document store vendor that was
 * configured does not belong to one of the {@link DocumentStoreVendor}.
 */
public class DocumentStoreNotSupportedException extends
    UnsupportedOperationException {

  /**
   * Constructs exception with the specified detail message.
   * @param message detailed message.
   */
  public DocumentStoreNotSupportedException(String message) {
    super(message);
  }
}