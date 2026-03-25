// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * 为YARN RM的/apps REST API提供灵活的字段过滤能力，支持客户端指定不返回哪些字段
 * 可通过扩展枚举类型新增需要排除的字段，无需修改核心解析逻辑
 * 扩展方式：1. 新增DeSelectType枚举项并指定字符串字面量 2. 业务层通过contains方法判断后执行过滤逻辑
 */
public class DeSelectFields {
  private static final Logger LOG =
      LoggerFactory.getLogger(DeSelectFields.class.getName());

  // 存储用户请求需要排除的所有字段类型
  private final Set<DeSelectType> types;

  public DeSelectFields() {
    this.types = new HashSet<DeSelectType>();
  }

  /**
   * 根据用户传入的待排除字段集合，初始化当前过滤对象
   * @param unselectedFields 用户请求中指定的待排除字段集合
   */
  public void initFields(Set<String> unselectedFields) {
    if (unselectedFields == null) {
      return;
    }
    // 遍历所有传入的待排除字段
    for (String field : unselectedFields) {
      // 跳过空字符串
      if (!field.trim().isEmpty()) {
        // 按逗号分割多个字段
        String[] literalsArray = field.split(",");
        // 遍历每个字段字面量
        for (String literals : literalsArray) {
          // 跳过空值和空字符串
          if (literals != null && !literals.trim().isEmpty()) {
            // 根据字面量获取对应的枚举类型
            DeSelectType type = DeSelectType.obtainType(literals);
            if (type == null) {
              // 字段不合法，记录警告日志并抛出参数错误异常
              LOG.warn("Invalid deSelects string " + literals.trim());
              DeSelectType[] typeArray = DeSelectType.values();
              String allSupportLiterals = Arrays.toString(typeArray);
              throw new BadRequestException("Invalid deSelects string "
                  + literals.trim() + " specified. It should be one of "
                  + allSupportLiterals);
            } else {
              // 合法字段加入待过滤集合
              this.types.add(type);
            }
          }
        }
      }
    }
  }

  /**
   * 判断指定字段类型是否需要被排除过滤
   * @param type 待检查的排除类型
   * @return true表示该字段需要排除，false表示需要保留返回
   */
  public boolean contains(DeSelectType type) {
    return types.contains(type);
  }

  /**
   * 可扩展的需要排除的字段类型枚举
   */
  public enum DeSelectType {

    /**
     * 排除资源请求字段，YARN-6280首次引入该类型
     */
    RESOURCE_REQUESTS("resourceRequests"),
    /**
     * 以下类型由YARN-6871引入，分别对应：超时信息、应用节点标签表达式、AM节点标签表达式、资源信息
     */
    TIMEOUTS("timeouts"),
    APP_NODE_LABEL_EXPRESSION("appNodeLabelExpression"),
    AM_NODE_LABEL_EXPRESSION("amNodeLabelExpression"),
    RESOURCE_INFO("resourceInfo");

    // 对应URL参数中的字符串字面量
    private final String literals;

    DeSelectType(String literals) {
      this.literals = literals;
    }

    /**
     * 返回枚举对应的URL参数字面量
     * @return 字段字符串字面量
     */
    @Override
    public String toString() {
      return literals;
    }

    /**
     * 根据URL参数传入的字符串字面量，获取对应的枚举类型
     * @param literals URL中deSelects参数传入的字段字面量
     * @return 匹配到的枚举类型，不匹配则返回null
     */
    public static DeSelectType obtainType(String literals) {
      // 忽略大小写匹配
      for (DeSelectType type : values()) {
        if (type.literals.equalsIgnoreCase(literals.trim())) {
          return type;
        }
      }
      return null;
    }
  }
}