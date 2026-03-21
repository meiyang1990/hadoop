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
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Objects;

/**
 * 辅助服务文件描述，定义需要在辅助服务容器中作为卷提供的配置文件信息
 * 用于YARN NodeManager辅助服务容器化场景，描述需要为容器准备的配置文件元数据
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuxServiceFile {

  /**
   * 辅助服务文件类型枚举
   **/
  public enum TypeEnum {
    /**静态文件类型*/
    STATIC("STATIC"),
    /**归档文件类型*/
    ARCHIVE("ARCHIVE");

    private String value;

    TypeEnum(String type) {
      this.value = type;
    }

    @Override
    @JsonValue
    public String toString() {
      return value;
    }
  }

  private TypeEnum type = null;
  private String srcFile = null;

  /**
   * 设置文件类型，支持xml、properties、json、yaml、template等标准格式
   * @param t 文件类型枚举
   * @return 当前AuxServiceFile实例，支持链式调用
   **/
  public AuxServiceFile type(TypeEnum t) {
    this.type = t;
    return this;
  }

  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  /**
   * 设置源文件路径，源文件会经过变量替换后转储到容器目标路径
   * 源文件通常由配置管理工具(Puppet/Chef)或HDFS等统一存储维护，当前仅支持HDFS
   * @param file 源文件路径
   * @return 当前AuxServiceFile实例，支持链式调用
   **/
  public AuxServiceFile srcFile(String file) {
    this.srcFile = file;
    return this;
  }

  @JsonProperty("src_file")
  public String getSrcFile() {
    return srcFile;
  }

  public void setSrcFile(String srcFile) {
    this.srcFile = srcFile;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuxServiceFile auxServiceFile = (AuxServiceFile) o;
    return Objects.equals(this.type, auxServiceFile.type)
        && Objects.equals(this.srcFile, auxServiceFile.srcFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, srcFile);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuxServiceFile {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    srcFile: ").append(toIndentedString(srcFile)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * 将对象转换为带缩进格式的字符串，便于日志输出可读性
   * @param o 需要转换的对象
   * @return 缩进格式化后的字符串
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}