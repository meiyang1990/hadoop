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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionQueueCapacitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionResourcesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.LI;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.UL;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

/**
 * Capacity调度器Web页面渲染器，负责生成容量调度器队列监控页面的HTML内容
 */
class CapacitySchedulerPage extends RmView {
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN =
      "left:0%;background:none;border:1px dashed #BFBFBF";
  static final String Q_AUTO_CREATED = "background:#F4F0CB";
  static final String Q_OVER = "background:#FFA333";
  static final String Q_UNDER = "background:#5BD75B";
  static final String ACTIVE_USER = "background:#FFFF00"; // Yellow highlight

  /**
   * 容量调度器队列信息请求作用域容器，保存当前请求的队列和标签信息
   */
  @RequestScoped
  static class CSQInfo {
    CapacitySchedulerInfo csinfo;
    CapacitySchedulerQueueInfo qinfo;
    String label;
    boolean isExclusiveNodeLabel;
  }

  /**
   * 叶子队列信息HTML块，负责渲染叶子队列的详细状态信息
   */
  static class LeafQueueInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;
    private String nodeLabel;

    @Inject LeafQueueInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
      nodeLabel = info.label;
    }

    @Override
    protected void render(Block html) {
      if (nodeLabel == null) {
        renderLeafQueueInfoWithoutParition(html);
      } else {
        renderLeafQueueInfoWithPartition(html);
      }
    }

    /**
     * 渲染带节点分区标签的叶子队列信息
     */
    private void renderLeafQueueInfoWithPartition(Block html) {
      String nodeLabelDisplay = nodeLabel.length() == 0
          ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : nodeLabel;
      // 首先展示分区特定的队列详情
      ResponseInfo ri =
          info("\'" + lqinfo.getQueuePath()
              + "\' Queue Status for Partition \'" + nodeLabelDisplay + "\'");
      renderQueueCapacityInfo(ri, nodeLabel);
      html.__(InfoBlock.class);
      // 清空信息，避免多个队列信息累加
      ri.clear();

      // 然后展示队列通用详情
      ri =
          info("\'" + lqinfo.getQueuePath() + "\' Queue Status")
              .__("Queue State:", lqinfo.getQueueState());
      renderCommonLeafQueueInfo(ri);

      html.__(InfoBlock.class);
      // 清空信息，避免多个队列信息累加
      ri.clear();
    }

    /**
     * 渲染不带分区信息的叶子队列信息
     */
    private void renderLeafQueueInfoWithoutParition(Block html) {
      ResponseInfo ri =
          info("\'" + lqinfo.getQueuePath() + "\' Queue Status")
              .__("Queue State:", lqinfo.getQueueState());
      renderQueueCapacityInfo(ri, "");
      renderCommonLeafQueueInfo(ri);
      html.__(InfoBlock.class);
      // 清空信息，避免多个队列信息累加
      ri.clear();
    }

    /**
     * 渲染队列容量相关信息到响应信息对象
     */
    private void renderQueueCapacityInfo(ResponseInfo ri, String label) {
      PartitionQueueCapacitiesInfo capacities =
          lqinfo.getCapacities().getPartitionQueueCapacitiesInfo(label);
      PartitionResourcesInfo resourceUsages =
          lqinfo.getResources().getPartitionResourceUsageInfo(label);

      // 从第一个用户获取用户级AM资源限制
      ResourceInfo userAMResourceLimit = null;
      ArrayList<UserInfo> usersList = lqinfo.getUsers().getUsersList();
      if (!usersList.isEmpty()) {
        userAMResourceLimit = resourceUsages.getUserAmLimit();
      }
      // 如果没有用户或无用户级限制，使用队列级AM限制
      if (userAMResourceLimit == null) {
        userAMResourceLimit = resourceUsages.getAMLimit();
      }
      ResourceInfo amUsed = (resourceUsages.getAmUsed() == null)
          ? new ResourceInfo(Resources.none())
          : resourceUsages.getAmUsed();
      ri.
          __("Used Capacity:",
              appendPercent(resourceUsages.getUsed(),
                  capacities.getUsedCapacity() / 100))
          .__(capacities.getWeight() != -1 ?
              "Configured Weight:" :
                  "Configured Capacity:",
              capacities.getWeight() != -1 ?
                  capacities.getWeight() :
                  capacities.getConfiguredMinResource() == null ?
                  Resources.none().toString() :
                  capacities.getConfiguredMinResource().toString())
          .__("Configured Max Capacity:",
              (capacities.getConfiguredMaxResource() == null
                  || capacities.getConfiguredMaxResource().getResource()
                      .equals(Resources.none()))
                          ? "unlimited"
                          : capacities.getConfiguredMaxResource().toString())
          .__("Effective Capacity:",
              appendPercent(capacities.getEffectiveMinResource(),
                  capacities.getCapacity() / 100))
          .__("Effective Max Capacity:",
              appendPercent(capacities.getEffectiveMaxResource(),
                  capacities.getMaxCapacity() / 100))
          .__("Absolute Used Capacity:",
              percent(capacities.getAbsoluteUsedCapacity() / 100))
          .__("Absolute Configured Capacity:",
              percent(capacities.getAbsoluteCapacity() / 100))
          .__("Absolute Configured Max Capacity:",
              percent(capacities.getAbsoluteMaxCapacity() / 100))
          .__("Used Resources:", resourceUsages.getUsed().toString())
          .__("Configured Max Application Master Limit:",
              StringUtils.format("%.1f", capacities.getMaxAMLimitPercentage()))
          .__("Max Application Master Resources:",
              resourceUsages.getAMLimit().toString())
          .__("Used Application Master Resources:", amUsed.toString())
          .__("Max Application Master Resources Per User:",
              userAMResourceLimit.toString());
    }

    /**
     * 渲染叶子队列通用信息到响应信息对象
     */
    private void renderCommonLeafQueueInfo(ResponseInfo ri) {
      ri.
          __("Num Schedulable Applications:",
              Integer.toString(lqinfo.getNumActiveApplications())).
          __("Num Non-Schedulable Applications:",
              Integer.toString(lqinfo.getNumPendingApplications())).
          __("Num Containers:",
              Integer.toString(lqinfo.getNumContainers())).
          __("Max Applications:",
              Integer.toString(lqinfo.getMaxApplications())).
          __("Max Applications Per User:",
              Integer.toString(lqinfo.getMaxApplicationsPerUser())).
          __("Configured Minimum User Limit Percent:",
              lqinfo.getUserLimit() + "%").
          __("Configured User Limit Factor:", lqinfo.getUserLimitFactor()).
          __("Accessible Node Labels:",
              StringUtils.join(",", lqinfo.getNodeLabels())).
          __("Ordering Policy: ", lqinfo.getOrderingPolicyDisplayName()).
          __("Preemption:",
              lqinfo.getPreemptionDisabled() ? "disabled" : "enabled").
          __("Intra-queue Preemption:", lqinfo.getIntraQueuePreemptionDisabled()
                  ? "disabled" : "enabled").
          __("Default Node Label Expression:",
              lqinfo.getDefaultNodeLabelExpression() == null
                  ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION
                  : lqinfo.getDefaultNodeLabelExpression()).
          __("Default Application Priority:",
              Integer.toString(lqinfo.getDefaultApplicationPriority()));
    }
  }

  /**
   * 队列用户信息HTML块，负责渲染叶子队列下所有活跃用户的资源使用信息
   */
  static class QueueUsersInfoBlock extends HtmlBlock {
    final CapacitySchedulerLeafQueueInfo lqinfo;
    private String nodeLabel;

    @Inject
    QueueUsersInfoBlock(ViewContext ctx, CSQInfo info) {
      super(ctx);
      lqinfo = (CapacitySchedulerLeafQueueInfo) info.qinfo;
      nodeLabel = info.label;
    }

    @Override
    protected void render(Block html) {
      TBODY<TABLE<Hamlet>> tbody =
          html.table("#userinfo").thead().$class("ui-widget-header").tr().th()
              .$class("ui-state-default").__("User Name").__().th()
              .$class("ui-state-default").__("User Limit Resource").__().th()
              .$class("ui-state-default").__("Weight").__().th()
              .$class("ui-state-default").__("Used Resource").__().th()
              .$class("ui-state-default").__("Max AM Resource").__().th()
              .$class("ui-state-default").__("Used AM Resource").__().th()
              .$class("ui-state-default").__("Schedulable Apps").__().th()
              .$class("ui-state-default").__("Non-Schedulable Apps").__().__().__()
              .tbody();

      PartitionResourcesInfo queueUsageResources =
          lqinfo.getResources().getPartitionResourceUsageInfo(
              nodeLabel == null ? "" : nodeLabel);

      ArrayList<UserInfo> users = lqinfo.getUsers().getUsersList();
      // 遍历每个用户渲染信息行
      for (UserInfo userInfo : users) {
        ResourceInfo resourcesUsed = userInfo.getResourcesUsed();
        ResourceInfo userAMLimitPerPartition =
            queueUsageResources.getUserAmLimit();
        // 如果没有用户级AM限制，使用队列级限制
        if (userAMLimitPerPartition == null) {
          userAMLimitPerPartition = queueUsageResources.getAMLimit();
        }
        // 根据用户权重调整AM限制
        if (userInfo.getUserWeight() != 1.0) {
          userAMLimitPerPartition =
              new ResourceInfo(
                  Resources.multiply(userAMLimitPerPartition.getResource(),
                      userInfo.getUserWeight()));
        }
        // 如果指定分区，获取分区下的资源使用
        if (nodeLabel != null) {
          resourcesUsed = userInfo.getResourceUsageInfo()
              .getPartitionResourceUsageInfo(nodeLabel).getUsed();
        }
        // 处理AM使用资源空值
        ResourceInfo amUsed = userInfo.getAMResourcesUsed();
        if (amUsed == null) {
          amUsed = new ResourceInfo(Resources.none());
        }
        // 高亮当前请求资源的活跃用户
        String highlightIfAsking =
            userInfo.getIsActive() ? ACTIVE_USER : null;
        tbody.tr().$style(highlightIfAsking).td(userInfo.getUsername())
            .td(userInfo.getUserResourceLimit().toString())
            .td(String.valueOf(userInfo.getUserWeight()))
            .td(resourcesUsed.toString())
            .td(userAMLimitPerPartition.toString())
            .td(amUsed.toString())
            .td(Integer.toString(userInfo.getNumActiveApplications()))
            .td(Integer.toString(userInfo.getNumPendingApplications())).__();
      }

      html.div().$class("usersinfo").h5("Active Users Info").__();
      tbody.__().__();
    }
  }

  /**
   * 队列树HTML块，递归渲染队列层级结构
   */
  public static class QueueBlock extends HtmlBlock {
    final CSQInfo csqinfo;

    @Inject QueueBlock(CSQInfo info) {
      csqinfo = info;
    }

    @Override
    public void render(Block html) {
      // 获取当前层级的子队列列表
      ArrayList<CapacitySchedulerQueueInfo> subQueues = (csqinfo.qinfo == null)
          ? csqinfo.csinfo.getQueues().getQueueInfoList()
          : csqinfo.qinfo.getQueues().getQueueInfoList();

      UL<Hamlet> ul = html.ul("#pq");
      float used;
      float absCap;
      float absMaxCap;
      float absUsedCap;
      // 遍历每个子队列渲染
      for (CapacitySchedulerQueueInfo info : subQueues) {
        String nodeLabel = (csqinfo.label == null) ? "" : csqinfo.label;
        // 独占节点标签过滤：只显示配置了该标签的队列
        if (!nodeLabel.isEmpty()
            && csqinfo.isExclusiveNodeLabel
            && !info.getNodeLabels().contains("*")
            && !info.getNodeLabels().contains(nodeLabel)) {
          continue;
        }
        PartitionQueueCapacitiesInfo partitionQueueCapsInfo = info
            .getCapacities().getPartitionQueueCapacitiesInfo(nodeLabel);
        used = partitionQueueCapsInfo.getUsedCapacity() / 100;
        absCap = partitionQueueCapsInfo.getAbsoluteCapacity() / 100;
        absMaxCap = partitionQueueCapsInfo.getAbsoluteMaxCapacity() / 100;
        absUsedCap = partitionQueueCapsInfo.getAbsoluteUsedCapacity() / 100;

        // 判断是否是自动创建的叶子队列，应用不同样式
        boolean isAutoCreatedLeafQueue = info.isLeafQueue() ?
            ((CapacitySchedulerLeafQueueInfo) info).isAutoCreatedLeafQueue()
            : false;
        float capPercent = absMaxCap == 0 ? 0 : absCap/absMaxCap;
        float usedCapPercent = absMaxCap == 0 ? 0 : absUsedCap/absMaxCap;

        String Q_WIDTH = width(absMaxCap * Q_MAX_WIDTH);
        LI<UL<Hamlet>> li = ul.
          li().
            a(_Q).$style(isAutoCreatedLeafQueue? join( Q_AUTO_CREATED, ";",
            Q_WIDTH)
            :  Q_WIDTH).
              $title(join("Absolute Capacity:", percent(absCap))).
              span().$style(join(Q_GIVEN, ";font-size:1px;", width(capPercent))).
            __('.').__().
              span().$style(join(width(usedCapPercent),
                ";font-size:1px;left:0%;", absUsedCap > absCap ? Q_OVER : Q_UNDER)).
            __('.').__().
              span(".q", info.getQueuePath()).__().
            span().$class("qstats").$style(left(Q_STATS_POS)).
            __(join(percent(used), " used")).__();

        csqinfo.qinfo