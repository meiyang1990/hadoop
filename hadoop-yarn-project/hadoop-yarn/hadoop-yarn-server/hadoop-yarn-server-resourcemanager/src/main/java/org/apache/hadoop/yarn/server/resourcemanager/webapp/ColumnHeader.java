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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

/**
 * YARN ResourceManager Web UI 列表表格列头模型，用于渲染HTML表格表头(TH标签)
 */
public class ColumnHeader {
  private String selector;
  private String cdata;

  /**
   * 构造列头对象
   * @param pselector CSS选择器名称，用于前端样式和选择
   * @param pcdata 列头显示文本内容
   */
  public ColumnHeader(String pselector, String pcdata) {
    this.selector = pselector;
    this.cdata = pcdata;
  }

  /**
   * 获取TH标签对应的CSS选择器
   * @return 选择器名称
   */
  public String getSelector() {
    return this.selector;
  }

  /**
   * 获取列头显示文本内容
   * @return 列头显示文本
   */
  public String getCData() {
    return this.cdata;
  }
}