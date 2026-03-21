// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

import java.text.NumberFormat;

/**
 * HBase时间线服务公共模块提供的工具类，包含时间线存储相关的通用工具方法。
 */
public final class HBaseTimelineSchemaUtils {
  /** 一天包含的毫秒数 */
  public static final long MILLIS_ONE_DAY = 86400000L;

  // 线程本地格式化实例，保证线程安全，应用ID格式化为4位最小宽度
  private static final ThreadLocal<NumberFormat> APP_ID_FORMAT =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          // 不使用千位分隔符
          fmt.setGroupingUsed(false);
          // 最小整数位数为4，不足补前导零
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  private HBaseTimelineSchemaUtils() {
  }

  /**
   * 将输入属性数组和聚合操作合并为新的属性数组。
   *
   * @param attributes 待合并的属性数组
   * @param aggOp 聚合操作
   * @return 合并后的属性数组
   */
  public static Attribute[] combineAttributes(Attribute[] attributes,
      AggregationOperation aggOp) {
    // 计算合并后的数组长度
    int newLength = getNewLengthCombinedAttributes(attributes, aggOp);
    // 创建新数组
    Attribute[] combinedAttributes = new Attribute[newLength];

    if (attributes != null) {
      // 复制原属性到新数组
      System.arraycopy(attributes, 0, combinedAttributes, 0, attributes.length);
    }

    if (aggOp != null) {
      // 将聚合操作的属性放到新数组末尾
      Attribute a2 = aggOp.getAttribute();
      combinedAttributes[newLength - 1] = a2;
    }
    return combinedAttributes;
  }

  /**
   * 计算属性数组和聚合操作合并后的总长度。
   *
   * @param attributes 属性数组
   * @param aggOp 聚合操作
   * @return 合并后的数组长度
   */
  private static int getNewLengthCombinedAttributes(Attribute[] attributes,
      AggregationOperation aggOp) {
    int oldLength = getAttributesLength(attributes);
    int aggLength = getAppOpLength(aggOp);
    return oldLength + aggLength;
  }

  /**
   * 获取聚合操作贡献的长度，非空则贡献1。
   */
  private static int getAppOpLength(AggregationOperation aggOp) {
    if (aggOp != null) {
      return 1;
    }
    return 0;
  }

  /**
   * 获取属性数组长度，空数组则返回0。
   */
  private static int getAttributesLength(Attribute[] attributes) {
    if (attributes != null) {
      return attributes.length;
    }
    return 0;
  }

  /**
   * 对整数取反，用于HBase行键排序，使得大值排在扫描结果最前面。
   *
   * @param key 原始整数键
   * @return 取反后的整数，保证最新值排在扫描结果首位
   */
  public static int invertInt(int key) {
    return Integer.MAX_VALUE - key;
  }

  /**
   * 根据输入时间戳获取当天起始（午夜00:00:00）的时间戳。
   *
   * @param ts 输入时间戳
   * @return 当天起始时间戳
   */
  public static long getTopOfTheDayTimestamp(long ts) {
    long dayTimestamp = ts - (ts % MILLIS_ONE_DAY);
    return dayTimestamp;
  }

  /**
   * 判断对象是否为整数类型（Short/Integer/Long）。
   *
   * @param obj 待判断对象
   * @return 是整数类型返回true，否则返回false
   */
  public static boolean isIntegralValue(Object obj) {
    return (obj instanceof Short) || (obj instanceof Integer) ||
        (obj instanceof Long);
  }

  /**
   * 将ApplicationId转换为字符串，为解决跨版本依赖兼容问题(YARN-6905)，不使用FastNumberFormat。
   *
   * @param appId 应用ID
   * @return 应用ID的字符串表示
   *
   */
  public static String convertApplicationIdToString(ApplicationId appId) {
    StringBuilder sb = new StringBuilder(64);
    sb.append(ApplicationId.appIdStrPrefix)
        .append("_")
        .append(appId.getClusterTimestamp())
        .append('_')
        .append(APP_ID_FORMAT.get().format(appId.getId()));
    return sb.toString();
  }
}