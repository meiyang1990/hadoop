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

import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

import java.util.Arrays;
import java.util.Set;

/**
 * 容量调度器队列映射规则匹配器集合，定义多种匹配器实现并提供工厂方法创建匹配器。
 * 用于YARN应用提交时根据规则匹配目标队列。
 */
public class MappingRuleMatchers {
  /**
   * 工具类，私有构造函数防止实例化。
   */
  private MappingRuleMatchers() {}

  /**
   * 全匹配匹配器，匹配所有应用提交请求。
   */
  public static class MatchAllMatcher implements MappingRuleMatcher {
    /**
     * 始终返回true，匹配所有应用提交。
     * @param variables 变量上下文，包含所有可用变量
     * @return true
     */
    @Override
    public boolean match(VariableContext variables) {
      return true;
    }

    @Override
    public String toString() {
      return "MatchAllMatcher";
    }
  }

  /**
   * 变量匹配器，检查指定上下文变量是否与给定值匹配。
   * 匹配值本身也可以包含变量，匹配前会先进行变量替换。
   */
  public static class VariableMatcher implements MappingRuleMatcher {
    /**
     * 待检查的上下文变量名称。
     */
    private String variable;
    /**
     * 用于匹配的目标值，可包含占位变量。
     */
    private String value;

    VariableMatcher(String variable, String value) {
      this.variable = variable;
      this.value = value == null ? "" : value;
    }

    /**
     * 先对目标值进行变量替换，再与上下文变量的值进行相等匹配。
     * 如果待检查变量不存在则返回false。
     * @param variables 变量上下文，包含所有可用变量
     * @return 变量值匹配返回true，否则返回false
     */
    @Override
    public boolean match(VariableContext variables) {
      if (variable == null) {
        return false;
      }
      // 对目标值中的变量进行替换
      String substituted = variables.replaceVariables(value);
      // 获取变量原始值（未替换的）
      String originalVariableValue = variables.getOriginal(variable);
      if (originalVariableValue != null) {
        return substituted.equals(originalVariableValue);
      }
      // 如果没有原始值则使用替换后的值比较
      return substituted.equals(variables.get(variable));
    }

    @Override
    public String toString() {
      return "VariableMatcher{" +
        "variable='" + variable + '\'' +
        ", value='" + value + '\'' +
        '}';
    }
  }

  /**
   * 用户组匹配器，检查提交应用的用户是否属于指定用户组。
   * 不区分主组还是辅助组，只要用户是该组成员即匹配成功。
   */
  public static class UserGroupMatcher implements MappingRuleMatcher {
    /**
     * 待匹配的目标用户组名称，可包含占位变量。
     */
    private String group;

    UserGroupMatcher(String value) {
      this.group = value;
    }

    /**
     * 检查用户是否属于目标用户组，匹配成功返回true。
     * 需要变量上下文中存在用户组数据集，如果不存在则返回false。
     * 如果目标组为null也返回false。
     * @param variables 变量上下文，包含所有可用变量
     * @return 用户属于目标组返回true，否则返回false
     */
    @Override
    public boolean match(VariableContext variables) {
      // 从上下文获取用户所属所有用户组集合
      Set<String> groups = variables.getExtraDataset("groups");

      if (group == null || groups == null) {
        return false;
      }
      // 替换目标组名称中的变量
      String substituted = variables.replaceVariables(group);
      return groups.contains(substituted);
    }

    @Override
    public String toString() {
      return "GroupMatcher{" +
          "group='" + group + '\'' +
          '}';
    }
  }

  /**
   * 逻辑与复合匹配器，所有子匹配器都匹配成功才返回true。
   */
  public static class AndMatcher implements MappingRuleMatcher {
    /**
     * 待检查的子匹配器列表。
     */
    private MappingRuleMatcher[] matchers;

    /**
     * 构造方法。
     * @param matchers 待检查的子匹配器列表
     */
    AndMatcher(MappingRuleMatcher...matchers) {
      this.matchers = matchers;
    }

    /**
     * 遍历所有子匹配器，全部匹配成功才返回true，任意一个匹配失败即返回false。
     * @param variables 变量上下文，包含所有可用变量
     * @return 所有子匹配器都匹配返回true，否则返回false
     */
    @Override
    public boolean match(VariableContext variables) {
      for (MappingRuleMatcher matcher : matchers) {
        if (!matcher.match(variables)) {
          return false;
        }
      }

      return true;
    }

    @Override
    public String toString() {
      return "AndMatcher{" +
          "matchers=" + Arrays.toString(matchers) +
          '}';
    }
  }

  /**
   * 逻辑或复合匹配器，任意一个子匹配器匹配成功即返回true。
   */
  public static class OrMatcher implements MappingRuleMatcher {
    /**
     * 待检查的子匹配器列表。
     */
    private MappingRuleMatcher[] matchers;

    /**
     * 构造方法。
     * @param matchers 待检查的子匹配器列表
     */
    OrMatcher(MappingRuleMatcher...matchers) {
      this.matchers = matchers;
    }

    /**
     * 遍历所有子匹配器，任意一个匹配成功即返回true，全部失败才返回false。
     * @param variables 变量上下文，包含所有可用变量
     * @return 任意子匹配器匹配返回true，否则返回false
     */
    @Override
    public boolean match(VariableContext variables) {
      for (MappingRuleMatcher matcher : matchers) {
        if (matcher.match(variables)) {
          return true;
        }
      }

      return false;
    }

    @Override
    public String toString() {
      return "OrMatcher{" +
          "matchers=" + Arrays.toString(matchers) +
          '}';
    }
  }

  /**
   * 创建匹配用户名的变量匹配器工厂方法。
   * @param userName 待匹配的用户名
   * @return 绑定%user变量的变量匹配器
   */
  public static MappingRuleMatcher createUserMatcher(String userName) {
    return new VariableMatcher("%user", userName);
  }

  /**
   * 创建匹配用户组的匹配器工厂方法。
   * @param groupName 待匹配的用户组名称
   * @return 用户组匹配器实例
   */
  public static MappingRuleMatcher createUserGroupMatcher(String groupName) {
    return new UserGroupMatcher(groupName);
  }

  /**
   * 创建用户名+用户组的复合与匹配器工厂方法。
   * 只有用户名和用户组同时匹配才会匹配成功。
   * @param userName 待匹配的用户名
   * @param groupName 待匹配的用户组名称
   * @return 包含两个匹配器的逻辑与匹配器
   */
  public static MappingRuleMatcher createUserGroupMatcher(
      String userName, String groupName) {
    return new AndMatcher(
        createUserMatcher(userName),
        createUserGroupMatcher(groupName));
  }

  /**
   * 创建匹配应用名称的变量匹配器工厂方法。
   * @param name 待匹配的应用名称
   * @return 绑定%application变量的变量匹配器
   */
  public static MappingRuleMatcher createApplicationNameMatcher(String name) {
    return new VariableMatcher("%application", name);
  }


  /**
   * 创建全匹配匹配器工厂方法。
   * @return 全匹配匹配器实例
   */
  public static MappingRuleMatcher createAllMatcher() {
    return new MatchAllMatcher();
  }
}