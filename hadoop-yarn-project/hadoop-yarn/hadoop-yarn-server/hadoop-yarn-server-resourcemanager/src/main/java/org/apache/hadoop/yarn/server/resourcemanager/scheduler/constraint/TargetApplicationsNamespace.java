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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.SELF;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.NOT_SELF;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.APP_TAG;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.APP_ID;
import static org.apache.hadoop.yarn.api.records.AllocationTagNamespaceType.ALL;

/**
 * 分配标签命名空间描述类，用于AllocationTags分配标签约束调度，
 * 根据命名空间类型解析得到目标应用集合。
 */
public abstract class TargetApplicationsNamespace implements
    Evaluable<TargetApplications> {

  /** 命名路径分隔符 */
  public final static String NAMESPACE_DELIMITER = "/";

  private AllocationTagNamespaceType nsType;
  // 命名空间作用域采用延迟绑定，由eval方法初始化
  private Set<ApplicationId> nsScope;

  /**
   * 构造方法，指定命名空间类型。
   * @param allocationTagNamespaceType 命名空间类型
   */
  public TargetApplicationsNamespace(AllocationTagNamespaceType
      allocationTagNamespaceType) {
    this.nsType = allocationTagNamespaceType;
  }

  /**
   * 如果应用ID集合不为空，则设置命名空间作用域。
   * @param appIds 应用ID集合
   */
  protected void setScopeIfNotNull(Set<ApplicationId> appIds) {
    if (appIds != null) {
      this.nsScope = appIds;
    }
  }

  /**
   * 获取命名空间类型。
   * @return 命名空间类型
   */
  public AllocationTagNamespaceType getNamespaceType() {
    return nsType;
  }

  /**
   * 获取命名空间作用域，即符合条件的应用ID集合。
   * @return 符合条件的应用ID集合
   */
  public Set<ApplicationId> getNamespaceScope() {
    if (this.nsScope == null) {
      throw new IllegalStateException("Invalid namespace scope,"
          + " it is not initialized. Evaluate must be called before"
          + " a namespace can be consumed.");
    }
    return this.nsScope;
  }

  /**
   * 根据输入的目标应用上下文，延迟绑定命名空间作用域。
   * 仅self/not-self等需要动态计算范围的命名空间类型需要执行该步骤。
   *
   * @param target 目标应用上下文，包含当前应用ID等信息
   * @throws InvalidAllocationTagsQueryException 如果命名空间格式非法则抛出异常
   */
  @Override
  public void evaluate(TargetApplications target)
      throws InvalidAllocationTagsQueryException {
    // Sub-class needs to override this when it requires the eval step.
  }

  @Override
  public String toString() {
    return this.nsType.toString();
  }

  /**
   * 仅包含当前应用自身的命名空间。
   */
  public static class Self extends TargetApplicationsNamespace {

    public Self() {
      super(SELF);
    }

    @Override
    public void evaluate(TargetApplications target)
        throws InvalidAllocationTagsQueryException {
      if (target == null || target.getCurrentApplicationId() == null) {
        throw new InvalidAllocationTagsQueryException("Namespace Self must"
            + " be evaluated against a single application ID.");
      }
      ApplicationId applicationId = target.getCurrentApplicationId();
      setScopeIfNotNull(ImmutableSet.of(applicationId));
    }
  }

  /**
   * 包含除当前应用外所有其他应用的命名空间。
   */
  public static class NotSelf extends TargetApplicationsNamespace {

    private ApplicationId applicationId;

    public NotSelf() {
      super(NOT_SELF);
    }

    /**
     * 设置当前应用ID，用于后续排除计算。
     * @param appId 当前应用ID
     */
    public void setApplicationId(ApplicationId appId) {
      this.applicationId = appId;
    }

    public ApplicationId getApplicationId() {
      return this.applicationId;
    }

    @Override
    public void evaluate(TargetApplications target) {
      Set<ApplicationId> otherAppIds = target.getOtherApplicationIds();
      setScopeIfNotNull(otherAppIds);
    }
  }

  /**
   * 包含集群所有应用的命名空间。
   */
  public static class All extends TargetApplicationsNamespace {

    public All() {
      super(ALL);
    }
  }

  /**
   * 包含带有指定应用标签所有应用的命名空间。
   */
  public static class AppTag extends TargetApplicationsNamespace {

    private String applicationTag;

    public AppTag(String appTag) {
      super(APP_TAG);
      this.applicationTag = appTag;
    }

    @Override
    public void evaluate(TargetApplications target) {
      setScopeIfNotNull(target.getApplicationIdsByTag(applicationTag));
    }

    @Override
    public String toString() {
      return APP_TAG.toString() + NAMESPACE_DELIMITER + this.applicationTag;
    }
  }

  /**
   * 仅包含指定应用ID单个应用的命名空间。
   */
  public static class AppID extends TargetApplicationsNamespace {

    private ApplicationId targetAppId;
    // app-id命名空间需要额外指定一个应用ID作为参数
    public AppID(ApplicationId applicationId) {
      super(APP_ID);
      this.targetAppId = applicationId;
      setScopeIfNotNull(ImmutableSet.of(targetAppId));
    }

    @Override
    public String toString() {
      return APP_ID.toString() + NAMESPACE_DELIMITER + this.targetAppId;
    }
  }

  /**
   * 从字符串解析命名空间实例。
   *
   * @param namespaceStr 命名空间字符串
   * @return 解析得到的命名空间实例
   * @throws InvalidAllocationTagsQueryException 如果字符串格式非法则抛出异常
   */
  public static TargetApplicationsNamespace parse(String namespaceStr)
      throws InvalidAllocationTagsQueryException {
    // 空输入默认返回Self命名空间
    if (Strings.isNullOrEmpty(namespaceStr)) {
      return new Self();
    }

    // 归一化输入，处理多余分隔符
    List<String> nsValues = normalize(namespaceStr);
    // 第一个片段是命名空间前缀
    String nsPrefix = nsValues.get(0);
    AllocationTagNamespaceType allocationTagNamespaceType =
        fromString(nsPrefix);
    switch (allocationTagNamespaceType) {
    case SELF:
      return new Self();
    case NOT_SELF:
      return new NotSelf();
    case ALL:
      return new All();
    case APP_ID:
      if (nsValues.size() != 2) {
        throw new InvalidAllocationTagsQueryException(
            "Missing the application ID in the namespace string: "
                + namespaceStr);
      }
      String appIDStr = nsValues.get(1);
      return parseAppID(appIDStr);
    case APP_TAG:
      if (nsValues.size() != 2) {
        throw new InvalidAllocationTagsQueryException(
            "Missing the application tag in the namespace string: "
                + namespaceStr);
      }
      return new AppTag(nsValues.get(1));
    default:
      throw new InvalidAllocationTagsQueryException(
          "Invalid namespace string " + namespaceStr);
    }
  }

  /**
   * 从前缀字符串匹配命名空间类型。
   * @param prefix 前缀字符串
   * @return 匹配到的命名空间类型
   * @throws InvalidAllocationTagsQueryException 如果前缀不匹配任何类型则抛出异常
   */
  private static AllocationTagNamespaceType fromString(String prefix) throws
      InvalidAllocationTagsQueryException {
    for (AllocationTagNamespaceType type :
        AllocationTagNamespaceType.values()) {
      if(type.getTypeKeyword().equals(prefix)) {
        return type;
      }
    }

    Set<String> values = Arrays.stream(AllocationTagNamespaceType.values())
        .map(AllocationTagNamespaceType::toString)
        .collect(Collectors.toSet());
    throw new InvalidAllocationTagsQueryException(
        "Invalid namespace prefix: " + prefix
            + ", valid values are: " + String.join(",", values));
  }

  /**
   * 解析应用ID字符串生成AppID命名空间。
   * @param appIDStr 应用ID字符串
   * @return 解析得到的AppID命名空间
   * @throws InvalidAllocationTagsQueryException 如果应用ID格式非法则抛出异常
   */
  private static TargetApplicationsNamespace parseAppID(String appIDStr)
      throws InvalidAllocationTagsQueryException {
    try {
      ApplicationId applicationId = ApplicationId.fromString(appIDStr);
      return new AppID(applicationId);
    } catch (IllegalArgumentException e) {
      throw new InvalidAllocationTagsQueryException(
          "Invalid application ID for "
              + APP_ID.getTypeKeyword() + ": " + appIDStr);
    }
  }

  /**
   * 归一化命名空间字符串，切割并过滤空片段，校验格式合法性。
   *
   * @param namespaceStr 输入的命名空间字符串
   * @return 归一化后的片段列表
   * @throws InvalidAllocationTagsQueryException 如果格式不合法则抛出异常
   */
  private static List<String> normalize(String namespaceStr)
      throws InvalidAllocationTagsQueryException {
    List<String> result = new ArrayList<>();
    if (namespaceStr == null) {
      return result;
    }

    String[] nsValues = namespaceStr.split(NAMESPACE_DELIMITER);
    for (String str : nsValues) {
      if (!Strings.isNullOrEmpty(str)) {
        result.add(str);
      }
    }

    // 当前仅允许1段或2段格式：<前缀> 或 <前缀>/<值>
    if (result.size() == 0 || result.size() > 2) {
      throw new InvalidAllocationTagsQueryException("Invalid namespace string: "
          + namespaceStr + ", the syntax is <namespace_prefix> or"
          + " <namespace_prefix>/<namespace_value>");
    }

    return result;
  }
}