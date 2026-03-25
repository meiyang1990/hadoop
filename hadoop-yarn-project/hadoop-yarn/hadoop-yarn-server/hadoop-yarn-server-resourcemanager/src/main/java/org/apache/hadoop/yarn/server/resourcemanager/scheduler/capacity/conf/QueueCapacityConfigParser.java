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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * YARN容量调度器队列容量配置解析器，负责从队列配置字符串解析出QueueCapacityVector对象。
 * 支持统一容量（百分比/权重）和异构资源容量（多种资源绝对值/混合容量）两种语法格式，
 * 可通过新增Parser扩展新的语法格式，通过新增ResourceUnitCapacityType扩展新的容量类型。
 */
public class QueueCapacityConfigParser {
  // 统一容量格式正则：匹配数值+后缀格式
  private static final String UNIFORM_REGEX = "^([0-9.]+)(.*)";
  // 异构资源格式正则：匹配方括号包裹的键值对格式
  private static final String RESOURCE_REGEX = "^\\[([\\w\\.,\\-_%\\ /]+=[\\w\\.,\\-_%\\ /]+)+\\]$";

  // 异构资源格式编译后的Pattern对象
  private static final Pattern RESOURCE_PATTERN = Pattern.compile(RESOURCE_REGEX);
  // 统一容量格式编译后的Pattern对象
  private static final Pattern UNIFORM_PATTERN = Pattern.compile(UNIFORM_REGEX);
  // 浮点数字符正则，用于提取数值部分
  public static final String FLOAT_DIGIT_REGEX = "[0-9.]";

  // 已注册的解析器列表，按顺序尝试匹配
  private final List<Parser> parsers = new ArrayList<>();

  /**
   * 构造函数，注册默认支持的两种解析器。
   */
  public QueueCapacityConfigParser() {
    parsers.add(new Parser(RESOURCE_PATTERN, this::heterogeneousParser));
    parsers.add(new Parser(UNIFORM_PATTERN, this::uniformParser));
  }

  /**
   * 从队列容量配置字符串解析生成QueueCapacityVector对象。
   * @param capacityString 容量配置字符串
   * @param queuePath 目标队列路径
   * @return 解析完成的容量向量
   */
  public QueueCapacityVector parse(String capacityString, QueuePath queuePath) {
    // 根队列容量固定为100%
    if (queuePath.isRoot()) {
      return QueueCapacityVector.of(100f, ResourceUnitCapacityType.PERCENTAGE);
    }

    if (capacityString == null) {
      return new QueueCapacityVector();
    }
    // 去除配置字符串中的所有空格
    capacityString = capacityString.replaceAll(" ", "");

    // 按顺序尝试所有已注册的解析器
    for (Parser parser : parsers) {
      Matcher matcher = parser.regex.matcher(capacityString);
      if (matcher.find()) {
        return parser.parser.apply(matcher);
      }
    }

    // 无匹配格式返回空容量向量
    return new QueueCapacityVector();
  }

  /**
   * 统一容量格式解析器，处理百分比/权重等单值容量格式。
   * @param matcher 正则匹配结果，包含数值和后缀
   * @return 解析完成的容量向量
   */
  private QueueCapacityVector uniformParser(Matcher matcher) {
    ResourceUnitCapacityType capacityType = null;
    String value = matcher.group(1);
    if (matcher.groupCount() == 2) {
      String matchedSuffix = matcher.group(2);
      // 遍历所有容量类型匹配后缀
      for (ResourceUnitCapacityType suffix : ResourceUnitCapacityType.values()) {
        // 统一容量不支持绝对值格式
        if (suffix.equals(ResourceUnitCapacityType.ABSOLUTE)) {
          continue;
        }
        // 百分比可以省略%符号
        String uniformSuffix = suffix.getPostfix().replaceAll("%", "");
        if (uniformSuffix.equals(matchedSuffix)) {
          capacityType = suffix;
        }
      }
    }

    // 未匹配到有效容量类型返回空向量
    if (capacityType == null) {
      return new QueueCapacityVector();
    }

    return QueueCapacityVector.of(Float.parseFloat(value), capacityType);
  }

  /**
   * 异构资源容量解析器，处理多种资源混合配置、绝对值资源配置格式。
   * @param matcher 正则匹配结果，包含方括号包裹的资源配置
   * @return 解析完成的容量向量
   */
  private QueueCapacityVector heterogeneousParser(Matcher matcher) {
    QueueCapacityVector capacityVector = QueueCapacityVector.newInstance();

    /*
     * 绝对资源配置使用[]包裹，语法示例：[memory=4Gi,vcores=2]
     * 表示该队列分配4GiB内存和2个vcore
     */
    // 获取整个方括号匹配结果
    String bracketedGroup = matcher.group(0);
    // 提取方括号内部的配置内容
    bracketedGroup = bracketedGroup.substring(1, bracketedGroup.length() - 1);
    // 按逗号分割每个资源键值对
    for (String kvPair : bracketedGroup.trim().split(",")) {
      String[] splits = kvPair.split("=");

      // 仅处理合法的键值对
      if (splits.length > 1) {
        setCapacityVector(capacityVector, splits[0], splits[1]);
      }
    }

    return capacityVector;
  }

  /**
   * 解析单个资源键值对，设置到容量向量中。
   * @param resource 目标容量向量
   * @param resourceName 资源名称
   * @param resourceValue 资源值字符串
   */
  private void setCapacityVector(
      QueueCapacityVector resource, String resourceName, String resourceValue) {
    // 默认容量类型为绝对值
    ResourceUnitCapacityType capacityType = ResourceUnitCapacityType.ABSOLUTE;

    // 提取资源值后缀（单位或容量类型标记）
    String suffix = resourceValue.replaceAll(FLOAT_DIGIT_REGEX, "");
    if (!resourceValue.endsWith(suffix)) {
      return;
    }

    // 提取数值部分并解析
    float parsedResourceValue = Float.parseFloat(resourceValue.substring(
        0, resourceValue.length() - suffix.length()));
    float convertedValue = parsedResourceValue;

    // 如果后缀是已知单位（如Gi、Mi），转换为Mi单位
    if (!suffix.isEmpty() && UnitsConversionUtil.KNOWN_UNITS.contains(suffix)) {
      convertedValue = UnitsConversionUtil.convert(suffix, "Mi", (long) parsedResourceValue);
    } else {
      // 后缀匹配容量类型
      for (ResourceUnitCapacityType capacityTypeSuffix : ResourceUnitCapacityType.values()) {
        if (capacityTypeSuffix.getPostfix().equals(suffix)) {
          capacityType = capacityTypeSuffix;
        }
      }
    }

    // 将解析完成的资源设置到容量向量
    resource.setResource(resourceName, convertedValue, capacityType);
  }

  /**
   * 检查输入配置字符串是否为容量向量格式（异构资源格式）。
   * @param configuredCapacity 输入配置字符串
   * @return true如果是容量向量格式，否则返回false
   */
  public boolean isCapacityVectorFormat(String configuredCapacity) {
    if (configuredCapacity == null) {
      return false;
    }

    String formattedCapacityString = configuredCapacity.replaceAll(" ", "");
    return RESOURCE_PATTERN.matcher(formattedCapacityString).find();
  }

  /**
   * 解析器内部类，封装正则表达式和解析方法。
   */
  private static class Parser {
    private final Pattern regex;
    private final Function<Matcher, QueueCapacityVector> parser;

    Parser(Pattern regex, Function<Matcher, QueueCapacityVector> parser) {
      this.regex = regex;
      this.parser = parser;
    }
  }

}