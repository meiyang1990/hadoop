// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;

/**
 * 容器放置约束管理器的抽象服务基类，实现了PlacementConstraintManager接口基础能力，
 * 为YARN调度器提供容器放置约束的校验基础框架。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PlacementConstraintManagerService extends AbstractService
    implements PlacementConstraintManager {

  /** 日志记录器 */
  protected static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintManagerService.class);

  private PlacementConstraintManager placementConstraintManager = null;

  /**
   * 构造方法，初始化服务名称。
   */
  public PlacementConstraintManagerService() {
    super(PlacementConstraintManagerService.class.getName());
  }

  /**
   * 校验放置约束的整体合法性，先校验源标签格式，后续将补充约束本身校验。
   */
  @Override
  public boolean validateConstraint(Set<String> sourceTags,
      PlacementConstraint placementConstraint) {
    // 先校验源标签格式合法性
    if (!validateSourceTags(sourceTags)) {
      return false;
    }
    // TODO: 在此处补充约束本身的实际校验逻辑 (对应YARN-6621)
    // TODO: 补充约束可满足性检查逻辑
    return true;
  }

  /**
   * 校验放置约束关联的源分配标签格式合法性。当前仅支持每个约束关联单个分配标签。
   *
   * @param sourceTags 待校验的源分配标签集合
   * @return 格式合法返回true，否则返回false
   */
  protected boolean validateSourceTags(Set<String> sourceTags) {
    if (sourceTags.isEmpty()) {
      LOG.warn("A placement constraint cannot be associated with an empty "
          + "set of tags.");
      return false;
    }
    if (sourceTags.size() > 1) {
      LOG.warn("Only a single tag can be associated with a placement "
          + "constraint currently.");
      return false;
    }
    return true;
  }

  /**
   * 从已校验过的源标签集合中获取唯一的分配标签。必须在调用{@link #validateSourceTags}
   * 确认标签合法后才能调用该方法。
   *
   * @param sourceTags 已校验的源分配标签集合
   * @return 唯一的源分配标签
   */
  protected String getValidSourceTag(Set<String> sourceTags) {
    return sourceTags.iterator().next();
  }

}