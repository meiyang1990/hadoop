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
package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;

import java.util.List;

/**
 * 容量调度器映射规则条件变量工厂类，存放各类条件变量实现，用于队列放置时动态解析路径变量
 */
public class MappingRuleConditionalVariables {
  /**
   * 工具类，隐藏构造方法
   */
  private MappingRuleConditionalVariables() {}

  /**
   * 次级用户组变量，实现%secondary_group条件变量的解析逻辑，用于根据已存在队列匹配用户非主组
   * 解析规则：
   * 1. 变量是路径第一个元素时：在根队列下查找第一个匹配的用户非主组队列
   * 2. 变量不是路径第一个元素时：在已解析的父级路径下查找第一个匹配的用户非主组队列
   */
  public static class SecondaryGroupVariable implements
      MappingRuleConditionalVariable {
    /**
     * 条件变量名称，即占位符字符串%secondary_group
     */
    public final static String VARIABLE_NAME = "%secondary_group";

    /**
     * 队列管理器实例，用于检查队列是否存在
     */
    private CapacitySchedulerQueueManager queueManager;
    /**
     * 待匹配的次级用户组候选列表，已排除主用户组
     */
    private List<String> potentialGroups;

    /**
     * 构造方法，初始化队列管理器和候选用户组列表
     * @param qm 用于检查队列存在性的队列管理器
     * @param groups 待匹配的次级用户组候选列表
     */
    public SecondaryGroupVariable(CapacitySchedulerQueueManager qm,
        List<String> groups) {
      queueManager = qm;
      potentialGroups = groups;
    }

    /**
     * 在队列路径中计算当前变量的值，返回匹配到的次级用户组名称
     * @param parts 拆分后的路径分段数组
     * @param currentIndex 当前变量在路径中的索引位置
     * @return 匹配到的次级用户组名称，无匹配返回空字符串
     */
    public String evaluateInPath(String[] parts, int currentIndex) {
      // 拼接已解析的父级路径前缀
      StringBuilder parentBuilder = new StringBuilder();
      // 遍历当前索引之前的所有路径分段，构建完整父路径
      for (int i = 0; i < currentIndex; i++) {
        parentBuilder.append(parts[i]);
        // 添加路径分隔符，最终父路径以分隔符结尾，方便拼接组名
        parentBuilder.append(".");
      }

      // 获取用于查找队列的完整前缀路径
      String lookupPrefix = parentBuilder.toString();

      // 遍历所有候选次级组，查找第一个已存在的匹配队列
      for (String group : potentialGroups) {
        String path = lookupPrefix + group;
        // 队列存在则返回当前组名作为变量值
        if (queueManager.getQueue(path) != null) {
          return group;
        }
      }

      // 未找到任何匹配队列，返回空字符串
      return "";
    }

    @Override
    public String toString() {
      return "SecondaryGroupVariable{" +
          "variableName='" + VARIABLE_NAME + "'," +
          "groups=" + potentialGroups +
          "}";
    }
  }

}