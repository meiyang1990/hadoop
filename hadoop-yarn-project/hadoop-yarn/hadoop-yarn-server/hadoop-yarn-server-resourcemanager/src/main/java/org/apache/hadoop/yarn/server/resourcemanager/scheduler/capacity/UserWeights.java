// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_SETTINGS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_WEIGHT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_WEIGHT_PATTERN;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

/**
 * 容量调度器队列用户权重管理器，存储并管理队列中单个用户的调度权重配置，
 * 用于实现基于权重的用户资源分配策略。
 */
public final class UserWeights {
  /** 默认用户权重，未单独配置时使用该值 */
  public static final float DEFAULT_WEIGHT = 1.0F;
  /**
   * Key: 用户名,
   * Value: 对应用户的权重浮点值。
   */
  private final Map<String, Float> data = new HashMap<>();

  private UserWeights() {}

  /** 创建空的用户权重实例 */
  public static UserWeights createEmpty() {
    return new UserWeights();
  }

  /**
   * 从容量调度器配置中解析并创建用户权重实例
   * @param conf 容量调度器配置对象
   * @param configurationProperties 配置属性集合
   * @param queuePath 当前队列路径
   * @return 解析完成的用户权重实例
   */
  public static UserWeights createByConfig(
      CapacitySchedulerConfiguration conf,
      ConfigurationProperties configurationProperties,
      QueuePath queuePath) {
    // 拼接当前队列用户设置配置前缀
    String queuePathPlusPrefix = getQueuePrefix(queuePath) + USER_SETTINGS;
    // 获取所有以当前前缀开头的配置项
    Map<String, String> props = configurationProperties
        .getPropertiesWithPrefix(queuePathPlusPrefix);

    UserWeights userWeights = new UserWeights();
    // 遍历所有匹配的配置项，提取用户权重
    for (Map.Entry<String, String> item: props.entrySet()) {
      // 使用正则匹配用户权重配置格式
      Matcher m = USER_WEIGHT_PATTERN.matcher(item.getKey());
      if (m.find()) {
        // 从配置键中提取用户名
        String userName = item.getKey().replaceFirst("\\." + USER_WEIGHT, "");
        if (!userName.isEmpty()) {
          // 替换配置中的通用变量
          String value = conf.substituteCommonVariables(item.getValue());
          // 存储解析后的用户权重
          userWeights.data.put(userName, new Float(value));
        }
      }
    }
    return userWeights;
  }

  /**
   * 根据用户名获取对应权重，未配置时返回默认权重
   * @param userName 用户名
   * @return 用户权重值
   */
  public float getByUser(String userName) {
    Float weight = data.get(userName);
    if (weight == null) {
      return DEFAULT_WEIGHT;
    }
    return weight;
  }

  /**
   * 验证叶队列所有用户权重的合法性，超出范围抛出异常
   * @param queueUserLimit 队列最大活跃用户数限制
   * @param queuePath 队列路径，用于异常信息展示
   * @throws IOException 当存在非法权重值时抛出
   */
  public void validateForLeafQueue(float queueUserLimit, String queuePath) throws IOException {
    // 遍历所有配置的用户权重进行校验
    for (Map.Entry<String, Float> e : data.entrySet()) {
      String userName = e.getKey();
      float weight = e.getValue();
      // 权重必须在0 和 100/queueUserLimit之间，避免单个用户占用过多资源
      if (weight < 0.0F || weight > (100.0F / queueUserLimit)) {
        throw new IOException("Weight (" + weight + ") for user \"" + userName
            + "\" must be between 0 and" + " 100 / " + queueUserLimit + " (= " +
            100.0f / queueUserLimit + ", the number of concurrent active users in "
            + queuePath + ")");
      }
    }
  }

  /**
   * 合并另一个UserWeights实例中的所有用户权重配置，覆盖重复项
   * @param addFrom 要合并的源UserWeights实例
   */
  public void addFrom(UserWeights addFrom) {
    data.putAll(addFrom.data);
  }
}