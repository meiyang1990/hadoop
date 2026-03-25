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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation;

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.*;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.util.List;
import java.util.Map;

/**
 * 公平调度队列分配文件解析器，负责从XML元素列表中加载队列配置属性
 */
public class AllocationFileQueueParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationFileQueueParser.class);

  public static final String ROOT = "root";
  public static final AccessControlList EVERYBODY_ACL =
      new AccessControlList("*");
  static final AccessControlList NOBODY_ACL = new AccessControlList(" ");
  private static final String MIN_RESOURCES = "minResources";
  private static final String MAX_RESOURCES = "maxResources";
  private static final String MAX_CHILD_RESOURCES = "maxChildResources";
  private static final String MAX_RUNNING_APPS = "maxRunningApps";
  private static final String MAX_AMSHARE = "maxAMShare";
  public static final String MAX_CONTAINER_ALLOCATION =
      "maxContainerAllocation";
  private static final String WEIGHT = "weight";
  private static final String MIN_SHARE_PREEMPTION_TIMEOUT =
      "minSharePreemptionTimeout";
  private static final String FAIR_SHARE_PREEMPTION_TIMEOUT =
      "fairSharePreemptionTimeout";
  private static final String FAIR_SHARE_PREEMPTION_THRESHOLD =
      "fairSharePreemptionThreshold";
  private static final String SCHEDULING_POLICY = "schedulingPolicy";
  private static final String SCHEDULING_MODE = "schedulingMode";
  private static final String ACL_SUBMIT_APPS = "aclSubmitApps";
  private static final String ACL_ADMINISTER_APPS = "aclAdministerApps";
  private static final String ACL_ADMINISTER_RESERVATIONS =
      "aclAdministerReservations";
  private static final String ACL_LIST_RESERVATIONS = "aclListReservations";
  private static final String ACL_SUBMIT_RESERVATIONS = "aclSubmitReservations";
  private static final String RESERVATION = "reservation";
  private static final String ALLOW_PREEMPTION_FROM = "allowPreemptionFrom";
  private static final String QUEUE = "queue";
  private static final String POOL = "pool";

  private final List<Element> elements;

  /**
   * 构造解析器，传入待解析的队列XML元素列表
   * @param elements 待解析的队列XML元素列表
   */
  public AllocationFileQueueParser(List<Element> elements) {
    this.elements = elements;
  }

  /**
   * 解析队列XML元素，生成队列配置属性集合
   * root队列可以配置也可以不配置，如果显式配置root，其他队列必须嵌套在root内部
   * @return 解析完成的队列配置属性集合
   * @throws AllocationConfigurationException 配置解析错误
   */
  public QueueProperties parse() throws AllocationConfigurationException {
    QueueProperties.Builder queuePropertiesBuilder =
        new QueueProperties.Builder();
    for (Element element : elements) {
      String parent = ROOT;
      if (element.getAttribute("name").equalsIgnoreCase(ROOT)) {
        if (elements.size() > 1) {
          throw new AllocationConfigurationException(
              "If configuring root queue,"
                  + " no other queues can be placed alongside it.");
        }
        parent = null;
      }
      loadQueue(parent, element, queuePropertiesBuilder);
    }

    return queuePropertiesBuilder.build();
  }

  /**
   * 从单个配置XML元素加载队列配置，递归加载子队列
   * @param parentName 父队列名称
   * @param element 当前队列XML元素
   * @param builder 队列配置属性构建器
   * @throws AllocationConfigurationException 配置解析错误
   */
  private void loadQueue(String parentName, Element element,
      QueueProperties.Builder builder) throws AllocationConfigurationException {
    String queueName =
        FairSchedulerUtilities.trimQueueName(element.getAttribute("name"));

    // 校验队列名称不包含点号
    if (queueName.contains(".")) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: queue name (" + queueName + ") shouldn't contain period.");
    }

    // 校验队列名称不为空
    if (queueName.isEmpty()) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: queue name shouldn't be empty or "
          + "consist only of whitespace.");
    }

    // 拼接完整队列路径（父队列名.当前队列名）
    if (parentName != null) {
      queueName = parentName + "." + queueName;
    }

    NodeList fields = element.getChildNodes();
    boolean isLeaf = true;
    boolean isReservable = false;
    boolean isMaxAMShareSet = false;

    // 遍历解析当前队列的所有子配置节点
    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      Element field = (Element) fieldNode;
      // 解析最小资源配置
      if (MIN_RESOURCES.equals(field.getTagName())) {
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text, 0L);
        builder.minQueueResources(queueName, val.getResource());
      } else if (MAX_RESOURCES.equals(field.getTagName())) {
        // 解析最大资源配置
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text);
        builder.maxQueueResources(queueName, val);
      } else if (MAX_CHILD_RESOURCES.equals(field.getTagName())) {
        // 解析所有子队列累计最大资源配置
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text);
        builder.maxChildQueueResources(queueName, val);
      } else if (MAX_RUNNING_APPS.equals(field.getTagName())) {
        // 解析最大同时运行应用数配置
        String text = getTrimmedTextData(field);
        int val = Integer.parseInt(text);
        builder.queueMaxApps(queueName, val);
      } else if (MAX_AMSHARE.equals(field.getTagName())) {
        // 解析ApplicationMaster最大资源占比，限制不超过1.0
        String text = getTrimmedTextData(field);
        float val = Float.parseFloat(text);
        val = Math.min(val, 1.0f);
        builder.queueMaxAMShares(queueName, val);
        isMaxAMShareSet = true;
      } else if (MAX_CONTAINER_ALLOCATION.equals(field.getTagName())) {
        // 解析单个容器最大分配资源配置
        String text = getTrimmedTextData(field);
        ConfigurableResource val =
            FairSchedulerConfiguration.parseResourceConfigValue(text);
        builder.queueMaxContainerAllocation(queueName, val.getResource());
      } else if (WEIGHT.equals(field.getTagName())) {
        // 解析公平调度权重配置
        String text = getTrimmedTextData(field);
        double val = Double.parseDouble(text);
        builder.queueWeights(queueName, (float) val);
      } else if (MIN_SHARE_PREEMPTION_TIMEOUT.equals(field.getTagName())) {
        // 解析最小资源抢占超时时间，转换为毫秒单位
        String text = getTrimmedTextData(field);
        long val = Long.parseLong(text) * 1000L;
        builder.minSharePreemptionTimeouts(queueName, val);
      } else if (FAIR_SHARE_PREEMPTION_TIMEOUT.equals(field.getTagName())) {
        // 解析公平份额抢占超时时间，转换为毫秒单位
        String text = getTrimmedTextData(field);
        long val = Long.parseLong(text) * 1000L;
        builder.fairSharePreemptionTimeouts(queueName, val);
      } else if (FAIR_SHARE_PREEMPTION_THRESHOLD.equals(field.getTagName())) {
        // 解析公平份额抢占阈值，限制在0~1之间
        String text = getTrimmedTextData(field);
        float val = Float.parseFloat(text);
        val = Math.max(Math.min(val, 1.0f), 0.0f);
        builder.fairSharePreemptionThresholds(queueName, val);
      } else if (SCHEDULING_POLICY.equals(field.getTagName())
          || SCHEDULING_MODE.equals(field.getTagName())) {
        // 解析调度策略，兼容旧版schedulingMode配置名
        String text = getTrimmedTextData(field);
        SchedulingPolicy policy = SchedulingPolicy.parse(text);
        builder.queuePolicies(queueName, policy);
      } else if (ACL_SUBMIT_APPS.equals(field.getTagName())) {
        // 解析提交应用ACL权限
        String text = ((Text) field.getFirstChild()).getData();
        builder.queueAcls(queueName, AccessType.SUBMIT_APP,
            new AccessControlList(text));
      } else if (ACL_ADMINISTER_APPS.equals(field.getTagName())) {
        // 解析管理队列ACL权限
        String text = ((Text) field.getFirstChild()).getData();
        builder.queueAcls(queueName, AccessType.ADMINISTER_QUEUE,
            new AccessControlList(text));
      } else if (ACL_ADMINISTER_RESERVATIONS.equals(field.getTagName())) {
        // 解析管理预约ACL权限
        String text = ((Text) field.getFirstChild()).getData();
        builder.reservationAcls(queueName,
            ReservationACL.ADMINISTER_RESERVATIONS,
            new AccessControlList(text));
      } else if (ACL_LIST_RESERVATIONS.equals(field.getTagName())) {
        // 解析查看预约ACL权限
        String text = ((Text) field.getFirstChild()).getData();
        builder.reservationAcls(queueName, ReservationACL.LIST_RESERVATIONS,
            new AccessControlList(text));
      } else if (ACL_SUBMIT_RESERVATIONS.equals(field.getTagName())) {
        // 解析提交预约ACL权限
        String text = ((Text) field.getFirstChild()).getData();
        builder.reservationAcls(queueName, ReservationACL.SUBMIT_RESERVATIONS,
            new AccessControlList(text));
      } else if (RESERVATION.equals(field.getTagName())) {
        // 标记队列支持预约，设置为父队列类型
        isReservable = true;
        builder.reservableQueues(queueName);
        builder.configuredQueues(FSQueueType.PARENT, queueName);
      } else if (ALLOW_PREEMPTION_FROM.equals(field.getTagName())) {
        // 解析是否允许从该队列抢占资源，false则标记为不可抢占
        String text = getTrimmedTextData(field);
        if (!Boolean.parseBoolean(text)) {
          builder.nonPreemptableQueues(queueName);
        }
      } else if (QUEUE.endsWith(field.getTagName())
          || POOL.equals(field.getTagName())) {
        // 递归解析子队列，标记当前队列为非叶子父队列
        loadQueue(queueName, field, builder);
        isLeaf = false;
      }
    }
    // 根据是否包含子队列和type属性，确定队列类型（叶子/父队列）
    if (isLeaf && !"parent".equals(element.getAttribute("type"))) {
      // 可预约队列已被标记为父队列，无需重复设置
      if (!isReservable) {
        builder.configuredQueues(FSQueueType.LEAF, queueName);
      }
    } else {
      // 父队列不允许配置 reservation 和 maxAMShare
      if (isReservable) {
        throw new AllocationConfigurationException(
            getErrorString(queueName, RESERVATION));
      } else if (isMaxAMShareSet) {
        throw new AllocationConfigurationException(
            getErrorString(queueName, MAX_AMSHARE));
      }
      builder.configuredQueues(FSQueueType.PARENT, queueName);
    }

    // 为未配置ACL的队列设置默认ACL
    // root队列默认所有用户都有所有权限，非root默认禁止所有访问
    for (QueueACL acl : QueueACL.values()) {
      AccessType accessType = SchedulerUtils.toAccessType(acl);
      if (!builder.isAclDefinedForAccessType(queueName, accessType)) {
        AccessControlList defaultAcl =
            queueName.equals(ROOT) ? EVERYBODY_ACL : NOBODY_ACL;
        builder.queueAcls(queueName, accessType, defaultAcl);
      }
    }

    // 校验最小资源不大于最大资源配置
    checkMinAndMaxResource(builder.getMinQueueResources(),
        builder.getMaxQueueResources(), queueName);
  }

  /**
   * 生成父队列非法配置的错误信息
   * @param parentQueueName 父队列名称
   * @param element 非法配置元素名
   * @return 格式化错误信息
   */
  private String getErrorString(String parentQueueName, String element) {
    return "The configuration settings"
        + " for " + parentQueueName + " are invalid. A queue element that "
        + "contains child queue elements or that has the type='parent' "
        + "attribute cannot also include a " + element + " element.";
  }

  /**
   * 获取XML元素文本内容并去除首尾空白
   * @param element XML元素
   * @return 修剪后的文本内容
   */
  private String getTrimmedTextData(Element element) {
    return ((Text) element.getFirstChild()).getData().trim();
  }

  /**
   * 校验队列最小资源不超过最大资源，若不满足打印警告日志
   * @param minResources 所有队列最小资源配置
   * @param maxResources 所有队列最大资源配置
   * @param queueName 当前校验队列名称
   */
  private void checkMinAndMaxResource(Map<String, Resource> minResources,
      Map<String, ConfigurableResource> maxResources, String queueName) {

    ConfigurableResource maxConfigurableResource = maxResources.get(queueName);
    Resource minResource = minResources.get(queueName);

    if (maxConfigurableResource != null && minResource != null) {
      Resource maxResource = maxConfigurableResource.getResource();

      // 当最大资源为绝对值时，检查最小资源是否不超过最大资源
      if (maxResource != null && !Resources.fitsIn(minResource, maxResource)) {
        LOG.warn(String.format(
            "Queue %s has max resources %s less than " + "min resources %s",
            queueName, maxResource, minResource));
      }
    }
  }
}