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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

/**
 * 本文件定义了数值类型转换器的扩展接口，为时间线服务HBase存储提供数值操作支持
 */

import java.util.Comparator;

/**
 * 扩展ValueConverter接口，为数值转换器提供比较、加法等数值操作能力
 * 用于时间线服务存储层中指标数值的聚合与比较
 */
public interface NumericValueConverter extends ValueConverter,
    Comparator<Number> {
  /**
   * 对两个或多个数值执行加法聚合
   * 输入为null时将其视为0处理
   *
   * @param num1 第一个要相加的数值
   * @param num2 第二个要相加的数值
   * @param numbers 后续需要相加的其他数值（可变参数）
   * @return 所有数值相加后的结果
   */
  Number add(Number num1, Number num2, Number...numbers);
}