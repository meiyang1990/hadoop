// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 辅助服务配置容器，用于存储可注入到服务组件的配置信息，支持通过环境变量、配置文件以及Docker辅助容器注入
 * 支持xml、properties、json、yaml等多种标准格式配置文件和模板文件
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuxServiceConfiguration {

  private Map<String, String> properties = new HashMap<>();
  private List<AuxServiceFile> files = new ArrayList<>();

  /**
   * 设置通用服务键值对配置，返回当前实例支持链式调用
   * @param props 通用服务属性键值对
   * @return 当前配置实例
   **/
  public AuxServiceConfiguration properties(Map<String, String> props) {
    this.properties = props;
    return this;
  }

  @JsonProperty("properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  /**
   * 设置需要注入到服务容器的配置文件列表，返回当前实例支持链式调用
   * @param fileList 需要注入的配置文件列表
   * @return 当前配置实例
   **/
  public AuxServiceConfiguration files(List<AuxServiceFile> fileList) {
    this.files = fileList;
    return this;
  }

  @JsonProperty("files")
  public List<AuxServiceFile> getFiles() {
    return files;
  }

  public void setFiles(List<AuxServiceFile> files) {
    this.files = files;
  }

  /**
   * 获取指定名称的配置属性，如果为空则返回默认值
   * @param name 属性名称
   * @param defaultValue 默认值
   * @return 属性值或默认值
   */
  public String getProperty(String name, String defaultValue) {
    String value = getProperty(name);
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return value;
  }

  /**
   * 设置单个配置属性
   * @param name 属性名称
   * @param value 属性值
   */
  public void setProperty(String name, String value) {
    properties.put(name, value);
  }

  /**
   * 获取单个配置属性，自动对属性名做trim处理
   * @param name 属性名称
   * @return 属性值
   */
  public String getProperty(String name) {
    return properties.get(name.trim());
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuxServiceConfiguration configuration = (AuxServiceConfiguration) o;
    return Objects.equals(this.properties, configuration.properties)
        && Objects.equals(this.files, configuration.files);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, files);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Configuration {\n");

    sb.append("    properties: ").append(toIndentedString(properties))
        .append("\n");
    sb.append("    files: ").append(toIndentedString(files)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * 将对象转换为带缩进的字符串，便于格式化输出
   * @param o 需要转换的对象
   * @return 带缩进的字符串
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}