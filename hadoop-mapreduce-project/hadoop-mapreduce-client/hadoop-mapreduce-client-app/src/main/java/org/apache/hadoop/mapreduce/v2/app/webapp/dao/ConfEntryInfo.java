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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * MapReduce应用Web服务配置项数据传输对象，用于在Web界面展示单个配置项的信息
 * 封装配置项的名称、值以及配置来源，支持JSON/XML序列化
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ConfEntryInfo {

  protected String name;
  protected String value;
  protected String[] source;

  /**
   * 默认无参构造函数，供JAXB序列化框架使用
   */
  public ConfEntryInfo() {
  }

  /**
   * 构造仅包含键值对的配置项信息
   * @param key 配置项名称
   * @param value 配置项值
   */
  public ConfEntryInfo(String key, String value) {
    this(key, value, null);
  }
  
  /**
   * 构造包含键值对和来源信息的完整配置项信息
   * @param key 配置项名称
   * @param value 配置项值
   * @param source 配置来源数组，表示该配置来自哪些配置文件
   */
  public ConfEntryInfo(String key, String value, String[] source) {
    this.name = key;
    this.value = value;
    this.source = source;
  }

  /**
   * 获取配置项名称
   * @return 配置项名称
   */
  public String getName() {
    return this.name;
  }

  /**
   * 获取配置项值
   * @return 配置项字符串值
   */
  public String getValue() {
    return this.value;
  }
  
  /**
   * 获取配置来源数组
   * @return 配置来源的字符串数组，每个元素表示一个配置文件路径
   */
  public String[] getSource() {
    return source;
  }
}