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
package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.AppIdKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;

/**
 * app_flow 表行键封装类，存储应用ID作为行键，用于HBase存储中应用到流关系的映射。
 */
public class AppToFlowRowKey {
  private final String appId;
  private final KeyConverter<String> appIdKeyConverter =
      new AppIdKeyConverter();

  /**
   * 构造方法，根据应用ID创建行键对象。
   * @param appId 应用ID
   */
  public AppToFlowRowKey(String appId) {
    this.appId = appId;
  }

  public String getAppId() {
    return appId;
  }

  /**
   * 构造 app_flow 表的完整行键字节数组。
   *
   * @return 序列化后的行键字节数组
   */
  public  byte[] getRowKey() {
    return appIdKeyConverter.encode(appId);
  }

  /**
   * 从字节数组反序列化解析出AppToFlowRowKey对象。
   *
   * @param rowKey 行键字节数组
   * @return 解析后的AppToFlowRowKey对象
   */
  public static AppToFlowRowKey parseRowKey(byte[] rowKey) {
    String appId = new AppIdKeyConverter().decode(rowKey);
    return new AppToFlowRowKey(appId);
  }
}