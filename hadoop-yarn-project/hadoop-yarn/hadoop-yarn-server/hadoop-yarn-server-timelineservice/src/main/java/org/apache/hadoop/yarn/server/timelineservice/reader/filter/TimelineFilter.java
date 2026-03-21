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

package org.apache.hadoop.yarn.server.timelineservice.reader.filter;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * 时间线服务查询过滤器抽象基类，所有具体过滤器都需要继承该类实现
 * 为时间线实体查询提供统一的过滤接口，支持多种过滤条件组合
 */
@Private
@Unstable
public abstract class TimelineFilter {

  /**
   * 定义过滤器支持的所有类型，用于区分不同过滤逻辑
   */
  @Private
  @Unstable
  public enum TimelineFilterType {
    /**
     * 组合多个过滤器，实现多条件的组合过滤
     */
    LIST,
    /**
     * 基于键值比较的过滤器，支持大于/小于等比较操作
     */
    COMPARE,
    /**
     * 基于键值相等匹配的过滤器
     */
    KEY_VALUE,
    /**
     * 基于键匹配多个值的过滤器，满足任意一个即命中
     */
    KEY_VALUES,
    /**
     * 前缀匹配过滤器，用于匹配配置项或指标名称前缀
     */
    PREFIX,
    /**
     * 存在性检查过滤器，用于检查指定键是否存在
     */
    EXISTS
  }

  /**
   * 获取当前过滤器的类型
   * @return 过滤器类型枚举值
   */
  public abstract TimelineFilterType getFilterType();

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}