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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 * 应用优先级ACL配置解析器，用于从容量调度器配置文件(capacity-scheduler.xml)
 * 解析应用优先级访问控制列表配置。
 */
public class AppPriorityACLConfigurationParser {

  private static final Logger LOG = LoggerFactory
      .getLogger(AppPriorityACLConfigurationParser.class);

  /**
   * 优先级ACL配置项类型枚举，定义支持的配置键类型。
   */
  public enum AppPriorityACLKeyType {
    USER(1), GROUP(2), MAX_PRIORITY(3), DEFAULT_PRIORITY(4);

    private final int id;

    AppPriorityACLKeyType(int id) {
      this.id = id;
    }

    public int getId() {
      return this.id;
    }
  }

  // 优先级ACL配置正则匹配模式，匹配[]包裹的ACL组
  public static final String PATTERN_FOR_PRIORITY_ACL = "\\[([^\\]]+)";

  @Private
  public static final String ALL_ACL = "*";

  @Private
  public static final String NONE_ACL = " ";

  /**
   * 解析优先级ACL配置字符串，生成解析后的优先级ACL组列表。
   * @param clusterMaxPriority 集群允许的最大优先级
   * @param aclString 原始ACL配置字符串
   * @return 解析完成的优先级ACL组列表
   */
  public List<AppPriorityACLGroup> getPriorityAcl(Priority clusterMaxPriority,
      String aclString) {

    List<AppPriorityACLGroup> aclList = new ArrayList<AppPriorityACLGroup>();
    Matcher matcher = Pattern.compile(PATTERN_FOR_PRIORITY_ACL)
        .matcher(aclString);

    /*
     * Each ACL group will be separated by "[]". Syntax of each ACL group could
     * be like below "user=b1,b2 group=g1 max-priority=a2 default-priority=a1"
     * Ideally this means "for this given user/group, maximum possible priority
     * is a2 and if the user has not specified any priority, then it is a1."
     */
    while (matcher.find()) {
      // 提取当前匹配到的ACL子组内容
      String aclSubGroup = matcher.group(1);
      if (aclSubGroup.trim().isEmpty()) {
        continue;
      }

      /*
       * Internal storage is PriorityACLGroup which stores each parsed priority
       * ACLs group. This will help while looking for a user to priority mapping
       * during app submission time. ACLs will be passed in below order only. 1.
       * user/group 2. max-priority 3. default-priority
       */
      AppPriorityACLGroup userPriorityACL = new AppPriorityACLGroup();

      // 临时存储用户和组ACL字符串，后续统一构建AccessControlList
      List<StringBuilder> userAndGroupName = new ArrayList<>();

      // 按空格分割键值对
      for (String kvPair : aclSubGroup.trim().split(" +")) {
        /*
         * There are 3 possible options for key here: 1. user/group 2.
         * max-priority 3. default-priority
         */
        // 按=分割键和值
        String[] splits = kvPair.split("=");

        // 确保是合法的键值对格式
        if (splits != null && splits.length > 1) {
          parsePriorityACLType(userPriorityACL, splits, userAndGroupName);
        }
      }

      // 如果配置的最大优先级超过集群最大优先级，重置为集群最大优先级
      if (userPriorityACL.getMaxPriority().getPriority() > clusterMaxPriority
          .getPriority()) {
        LOG.warn("ACL configuration for '" + userPriorityACL.getMaxPriority()
            + "' is greater that cluster max priority. Resetting ACLs to "
            + clusterMaxPriority);
        userPriorityACL.setMaxPriority(
            Priority.newInstance(clusterMaxPriority.getPriority()));
      }

      // 构建当前ACL组的访问控制列表
      AccessControlList acl = createACLStringForPriority(userAndGroupName);
      userPriorityACL.setACLList(acl);
      aclList.add(userPriorityACL);
    }

    return aclList;
  }

  /*
   * Parse different types of ACLs sub parts for on priority group and store in
   * a map for later processing.
   */
  /**
   * 解析单个优先级ACL配置项，根据配置键类型存储到对应位置。
   * @param userPriorityACL 目标优先级ACL组对象
   * @param splits 分割后的键值对数组
   * @param userAndGroupName 临时存储用户/组ACL的列表
   */
  private void parsePriorityACLType(AppPriorityACLGroup userPriorityACL,
      String[] splits, List<StringBuilder> userAndGroupName) {
    // 将配置键转换为枚举类型
    AppPriorityACLKeyType aclType = AppPriorityACLKeyType
        .valueOf(StringUtils.toUpperCase(splits[0].trim()));
    switch (aclType) {
    case MAX_PRIORITY :
      // 解析并设置最大优先级
      userPriorityACL
          .setMaxPriority(Priority.newInstance(Integer.parseInt(splits[1])));
      break;
    case USER :
      // 添加用户ACL到临时列表
      userAndGroupName.add(getUserOrGroupACLStringFromConfig(splits[1]));
      break;
    case GROUP :
      // 添加组ACL到临时列表
      userAndGroupName.add(getUserOrGroupACLStringFromConfig(splits[1]));
      break;
    case DEFAULT_PRIORITY :
      // 解析并设置默认优先级，负优先级重置为0
      int defaultPriority = Integer.parseInt(splits[1]);
      Priority priority = (defaultPriority < 0)
          ? Priority.newInstance(0)
          : Priority.newInstance(defaultPriority);
      userPriorityACL.setDefaultPriority(priority);
      break;
    default:
      break;
    }
  }

  /*
   * This method will help to append different types of ACLs keys against one
   * priority. For eg,USER will be appended with GROUP as "user2,user4 group1".
   */
  /**
   * 基于临时存储的用户和组ACL字符串，构建最终的AccessControlList对象。
   * @param acls 存储用户和组ACL的列表
   * @return 构建完成的访问控制列表对象
   */
  private AccessControlList createACLStringForPriority(
      List<StringBuilder> acls) {

    String finalACL = "";
    String userACL = acls.get(0).toString();

    // 如果用户ACL是通配符*，直接赋予所有用户访问权限
    // "user" is at index 0, and "group" is at index 1.
    if (userACL.trim().equals(ALL_ACL)) {
      finalACL = ALL_ACL;
    } else if (userACL.equals(NONE_ACL)) {
      finalACL = NONE_ACL;
    } else {

      // 添加用户ACL部分
      if (!userACL.trim().isEmpty()) {
        finalACL = acls.get(0).toString();
      }

      // 如果存在组ACL，添加组ACL部分
      if (acls.size() > 1) {
        String groupACL = acls.get(1).toString();
        if (!groupACL.trim().isEmpty()) {
          finalACL = finalACL + " "
              + acls.get(1).toString();
        }
      }
    }

    return new AccessControlList(finalACL.trim());
  }

  /*
   * This method will help to append user/group acl string against given
   * priority. For example "user1,user2 group1,group2"
   */
  /**
   * 从配置中提取用户/组ACL字符串，处理通配符情况。
   * @param value 配置中的值部分
   * @return 构建好的ACL字符串Builder
   */
  private StringBuilder getUserOrGroupACLStringFromConfig(String value) {

    StringBuilder aclTypeName = new StringBuilder();

    // 如果是通配符，直接返回*
    if (value.trim().equals(ALL_ACL)) {
      aclTypeName.setLength(0);
      aclTypeName.append(ALL_ACL);
      return aclTypeName;
    }

    // 否则直接返回修剪后的原值
    aclTypeName.append(value.trim());
    return aclTypeName;
  }
}