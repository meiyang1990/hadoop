// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.AbstractConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.And;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.Or;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.SingleConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression.TargetType;
import org.apache.hadoop.yarn.api.resource.PlacementConstraintTransformations.SingleConstraintTransformer;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE_PARTITION;

/**
 * 放置约束工具类，为放置算法提供公共工具方法，简化约束放置逻辑处理
 * 供 {@link DefaultPlacementAlgorithm} 放置算法使用
 */
@Public
@Unstable
public final class PlacementConstraintsUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintsUtil.class);

  // Suppresses default constructor, ensuring non-instantiability.
  private PlacementConstraintsUtil() {
  }

  /**
   * 检查单个分配标签约束是否被指定节点满足
   *
   * @param targetApplicationId 目标应用ID，可被分配标签中的目标应用ID覆盖
   * @param sc 单个放置约束
   * @param te 目标表达式
   * @param node 调度节点
   * @param tm 分配标签管理器
   * @return 是否满足约束
   * @throws InvalidAllocationTagsQueryException 查询格式异常
   */
  private static boolean canSatisfySingleConstraintExpression(
      ApplicationId targetApplicationId, SingleConstraint sc,
      TargetExpression te, SchedulerNode node, AllocationTagsManager tm)
      throws InvalidAllocationTagsQueryException {
    // 创建分配标签，供后续基数检查使用
    AllocationTags allocationTags = AllocationTags.createAllocationTags(
        targetApplicationId, te.getTargetKey(), te.getTargetValues());

    long minScopeCardinality = 0;
    long maxScopeCardinality = 0;

    // 优化：仅在需要时检查基数
    int desiredMinCardinality = sc.getMinCardinality();
    int desiredMaxCardinality = sc.getMaxCardinality();
    boolean checkMinCardinality = desiredMinCardinality > 0;
    boolean checkMaxCardinality = desiredMaxCardinality < Integer.MAX_VALUE;

    // 作用域为节点，获取节点上匹配分配标签的基数
    if (sc.getScope().equals(PlacementConstraints.NODE)) {
      if (checkMinCardinality) {
        minScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(),
            allocationTags, Long::min);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getNodeCardinalityByOp(node.getNodeID(),
            allocationTags, Long::max);
      }
    } else if (sc.getScope().equals(PlacementConstraints.RACK)) {
      // 作用域为机架，获取机架上匹配分配标签的基数
      if (checkMinCardinality) {
        minScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            allocationTags, Long::min);
      }
      if (checkMaxCardinality) {
        maxScopeCardinality = tm.getRackCardinalityByOp(node.getRackName(),
            allocationTags, Long::max);
      }
    }

    // 同时满足最小基数和最大基数要求才返回true
    return (desiredMinCardinality <= 0
        || minScopeCardinality >= desiredMinCardinality) && (
        desiredMaxCardinality == Integer.MAX_VALUE
            || maxScopeCardinality <= desiredMaxCardinality);
  }

  /**
   * 检查节点属性约束是否被指定节点满足
   *
   * @param sc 单个放置约束
   * @param targetExpression 目标表达式
   * @param schedulerNode 调度节点
   * @return 是否满足约束
   */
  private static boolean canSatisfyNodeConstraintExpression(
      SingleConstraint sc, TargetExpression targetExpression,
      SchedulerNode schedulerNode) {
    Set<String> values = targetExpression.getTargetValues();

    // 处理节点分区标签约束
    if (targetExpression.getTargetKey().equals(NODE_PARTITION)) {
      if (values == null || values.isEmpty()) {
        // 未指定分区，节点必须为无标签分区
        return schedulerNode.getPartition()
            .equals(RMNodeLabelsManager.NO_LABEL);
      } else {
        // 检查节点分区是否匹配要求
        String nodePartition = values.iterator().next();
        if (!nodePartition.equals(schedulerNode.getPartition())) {
          return false;
        }
      }
    } else {
      // 处理普通节点属性约束
      NodeAttributeOpCode opCode = sc.getNodeAttributeOpCode();
      // 获取请求的节点属性
      String inputAttribute = values.iterator().next();
      NodeAttribute requestAttribute = getNodeConstraintFromRequest(
          targetExpression.getTargetKey(), inputAttribute);
      if (requestAttribute == null) {
        return true;
      }

      // 根据操作码评估节点是否满足约束
      return getNodeConstraintEvaluatedResult(schedulerNode, opCode,
          requestAttribute);
    }
    return true;
  }

  /**
   * 根据节点属性和操作码评估节点是否满足约束
   *
   * @param schedulerNode 调度节点
   * @param opCode 属性操作码
   * @param requestAttribute 请求的节点属性
   * @return 是否满足约束
   */
  private static boolean getNodeConstraintEvaluatedResult(
      SchedulerNode schedulerNode,
      NodeAttributeOpCode opCode, NodeAttribute requestAttribute) {
    // 节点无属性或请求属性不存在于节点上
    if (schedulerNode.getNodeAttributes() == null ||
        !schedulerNode.getNodeAttributes().contains(requestAttribute)) {
      // 不等于操作符允许属性不存在，仍然接受该节点
      if (opCode == NodeAttributeOpCode.NE) {
        LOG.debug("Incoming requestAttribute:{} is not present in {},"
            + " however opcode is NE. Hence accept this node.",
            requestAttribute, schedulerNode.getNodeID());
        return true;
      }
      // 其他操作符，属性不存在直接拒绝
      LOG.debug("Incoming requestAttribute:{} is not present in {},"
          + " skip such node.", requestAttribute, schedulerNode.getNodeID());
      return false;
    }

    boolean found = false;
    // 遍历节点所有属性，检查是否有匹配的属性满足操作要求
    for (Iterator<NodeAttribute> it = schedulerNode.getNodeAttributes()
        .iterator(); it.hasNext();) {
      NodeAttribute nodeAttribute = it.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Starting to compare Incoming requestAttribute :"
            + requestAttribute
            + " with requestAttribute value= " + requestAttribute
            .getAttributeValue()
            + ", stored nodeAttribute value=" + nodeAttribute
            .getAttributeValue());
      }
      if (requestAttribute.equals(nodeAttribute)) {
        // 检查操作码是否匹配
        if (isOpCodeMatches(requestAttribute, nodeAttribute, opCode)) {
          LOG.debug("Incoming requestAttribute:{} matches with node:{}",
              requestAttribute, schedulerNode.getNodeID());
          found = true;
          return found;
        }
      }
    }
    // 未找到匹配属性，拒绝节点
    if (!found) {
      LOG.debug("skip this node:{} for requestAttribute:{}",
          schedulerNode.getNodeID(), requestAttribute);
      return false;
    }
    return true;
  }

  /**
   * 检查请求属性和节点属性是否匹配指定操作码
   *
   * @param requestAttribute 请求属性
   * @param nodeAttribute 节点属性
   * @param opCode 操作码
   * @return 是否匹配操作要求
   */
  private static boolean isOpCodeMatches(NodeAttribute requestAttribute,
      NodeAttribute nodeAttribute, NodeAttributeOpCode opCode) {
    boolean retCode = false;
    switch (opCode) {
    case EQ:
      // 等于：属性值必须相同
      retCode = requestAttribute.getAttributeValue()
          .equals(nodeAttribute.getAttributeValue());
      break;
    case NE:
      // 不等于：属性值必须不同
      retCode = !(requestAttribute.getAttributeValue()
          .equals(nodeAttribute.getAttributeValue()));
      break;
    default:
      break;
    }
    return retCode;
  }

  /**
   * 检查单个约束是否被指定节点满足，遍历所有目标表达式依次检查
   *
   * @param applicationId 应用ID
   * @param singleConstraint 单个约束
   * @param schedulerNode 调度节点
   * @param tagsManager 分配标签管理器
   * @param dcOpt 可选诊断收集器
   * @return 是否满足约束
   * @throws InvalidAllocationTagsQueryException 查询格式异常
   */
  private static boolean canSatisfySingleConstraint(ApplicationId applicationId,
      SingleConstraint singleConstraint, SchedulerNode schedulerNode,
      AllocationTagsManager tagsManager,
      Optional<DiagnosticsCollector> dcOpt)
      throws InvalidAllocationTagsQueryException {
    // 遍历所有目标表达式
    Iterator<TargetExpression> expIt =
        singleConstraint.getTargetExpressions().iterator();
    while (expIt.hasNext()) {
      TargetExpression currentExp = expIt.next();
      // 处理分配标签类型目标
      if (currentExp.getTargetType().equals(TargetType.ALLOCATION_TAG)) {
        // 检查约束是否满足，不满足则收集诊断信息并返回false
        if (!canSatisfySingleConstraintExpression(applicationId,
            singleConstraint, currentExp, schedulerNode, tagsManager)) {
          if (dcOpt.isPresent()) {
            dcOpt.get().collectPlacementConstraintDiagnostics(
                singleConstraint.build(), TargetType.ALLOCATION_TAG);
          }
          return false;
        }
      } else if (currentExp.getTargetType().equals(TargetType.NODE_ATTRIBUTE)) {
        // 处理节点属性类型目标，检查约束是否满足
        if (!canSatisfyNodeConstraintExpression(singleConstraint, currentExp,
            schedulerNode)) {
          if (dcOpt.isPresent()) {
            dcOpt.get().collectPlacementConstraintDiagnostics(
                singleConstraint.build(), TargetType.NODE_ATTRIBUTE);
          }
          return false;
        }
      }
    }
    // 所有目标表达式都满足，返回true
    return true;
  }

  /**
   * 检查AND组合约束是否满足：所有子约束都满足才返回true
   *
   * @param appId 应用ID
   * @param constraint AND约束
   * @param node 调度节点
   * @param atm 分配标签管理器
   * @param dcOpt 可选诊断收集器
   * @return 是否满足约束
   * @throws InvalidAllocationTagsQueryException 查询格式异常
   */
  private static boolean canSatisfyAndConstraint(ApplicationId appId,
      And constraint, SchedulerNode node, AllocationTagsManager atm,
      Optional<DiagnosticsCollector> dcOpt)
      throws InvalidAllocationTagsQueryException {
    // 遍历所有子约束，任一不满足则返回false
    for (AbstractConstraint child : constraint.getChildren()) {
      if(!canSatisfyConstraints(appId, child.build(), node, atm, dcOpt)) {
        return false;
      }
    }
    return true;
  }

  /**
   * 检查OR组合约束是否满足：任一子约束满足就返回true
   *
   * @param appId 应用ID
   * @param constraint OR约束
   * @param node 调度节点
   * @param atm 分配标签管理器
   * @param dcOpt 可选诊断收集器
   * @return 是否满足约束
   * @throws InvalidAllocationTagsQueryException 查询格式异常
   */
  private static boolean canSatisfyOrConstraint(ApplicationId appId,
      Or constraint, SchedulerNode node, AllocationTagsManager atm,
      Optional<DiagnosticsCollector> dcOpt)
      throws InvalidAllocationTagsQueryException {
    // 遍历所有子约束，任一满足则返回true
    for (AbstractConstraint child : constraint.getChildren()) {
      if (canSatisfyConstraints(appId, child.build(), node, atm, dcOpt)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 递归检查任意放置约束是否被指定节点满足
   *
   * @param appId 应用ID
   * @param constraint 放置约束
   * @param node 调度节点
   * @param atm 分配标签管理器
   * @param dcOpt 可选诊断收集器
   * @return 是否满足约束
   * @throws InvalidAllocationTagsQueryException 查询格式异常或不支持的约束类型
   */
  private static boolean canSatisfyConstraints(ApplicationId appId,
      PlacementConstraint constraint, SchedulerNode node,
      AllocationTagsManager atm,
      Optional<DiagnosticsCollector> dcOpt)
      throws InvalidAllocationTagsQueryException {
    // 空约束默认满足
    if (constraint == null) {
      LOG.debug("Constraint is found empty during constraint validation for"
          + " app:{}", appId);
      return true;
    }

    // 转换约束为SingleConstraint格式
    SingleConstraintTransformer singleTransformer =
        new SingleConstraintTransformer(constraint);
    constraint = singleTransformer.transform();
    AbstractConstraint sConstraintExpr = constraint.getConstraintExpr();

    // 根据约束类型分发到对应检查方法
    if (sConstraintExpr instanceof SingleConstraint) {
      SingleConstraint single = (SingleConstraint) sConstraintExpr;
      return canSatisfySingleConstraint(appId, single, node, atm, dcOpt);
    } else if (sConstraintExpr instanceof And) {
      And and = (And) sConstraintExpr;
      return canSatisfyAndConstraint(appId, and, node, atm, dcOpt);
    } else if (sConstraintExpr instanceof Or) {
      Or or = (Or) sConstraintExpr;
      return canSatisfyOrConstraint(appId, or, node, atm, dcOpt);
    } else {
      // 不支持的约束类型，抛出异常
      throw new InvalidAllocationTagsQueryException(
          "Unsupported type of constraint: "
              + sConstraintExpr.getClass().getSimpleName());
    }
  }

  /**
   * 检查调度请求的放置约束是否被指定节点当前满足
   * 遵循优先级检查：请求级约束 > 应用级约束 > 全局约束
   * 仅检查约束条件，不检查资源是否充足，资源检查由调度器后续完成
   *
   * @param applicationId 应用ID
   * @param request 调度请求
   * @param schedulerNode 调度节点
   * @param pcm 放置约束管理器
   * @param atm 分配标签管理器
   * @param dcOpt 可选诊断收集器
   * @return 是否满足约束
   * @throws InvalidAllocationTagsQueryException 查询格式异常
   */
  public static boolean canSatisfyConstraints(ApplicationId applicationId,
      SchedulingRequest request, SchedulerNode schedulerNode,
      PlacementConstraintManager pcm, AllocationTagsManager atm,
      Optional<DiagnosticsCollector> dcOpt)
      throws InvalidAllocationTagsQueryException {
    Set<String> sourceTags = null;
    PlacementConstraint pc = null;
    if (request != null) {
      sourceTags = request.getAllocationTags();
      pc = request.getPlacementConstraint();
    }
    // 获取多级合并后的约束并检查
    return canSatisfyConstraints(applicationId,
        pcm.getMultilevelConstraint(applicationId, sourceTags, pc),
        schedulerNode, atm, dcOpt);
  }

  /**
   * 无诊断收集器版本的约束检查
   *
   * @param applicationId 应用ID
   * @param request 调度请求
   * @param schedulerNode 调度节点
   * @param pcm 放置约束管理器
   *