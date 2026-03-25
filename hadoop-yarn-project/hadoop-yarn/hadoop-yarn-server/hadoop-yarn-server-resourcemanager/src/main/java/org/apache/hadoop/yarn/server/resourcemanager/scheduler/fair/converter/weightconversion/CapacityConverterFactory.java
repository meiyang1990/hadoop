// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

/**
 * 容量权重转换器工厂，根据配置创建不同的容量权重转换策略实现。
 * 用于公平调度器配置转换过程中，将权重转换为百分比或保留权重形式。
 */
public final class CapacityConverterFactory {
  private CapacityConverterFactory() {
    // 工具类不允许实例化
  }

  /**
   * 根据是否使用百分比模式获取对应的容量权重转换器实例。
   * @param usePercentage 是否使用百分比模式
   * @return 对应策略的容量权重转换器实例
   */
  public static CapacityConverter getConverter(
      boolean usePercentage) {
    return usePercentage ?
        new WeightToPercentConverter() : new WeightToWeightConverter();
  }
}