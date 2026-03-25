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

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfigurationException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 负责解析公平调度器的allocation.xml分配文件，提取全局配置、队列配置和用户配置。
 * 所有合法标签的文本值会存入textValues，其他有意义的字段会在parse()过程中解析保存。
 */
public class AllocationFileParser {
  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationFileParser.class);

  // 队列最大资源默认值标签
  private static final String QUEUE_MAX_RESOURCES_DEFAULT =
      "queueMaxResourcesDefault";
  // 用户最大运行应用数默认值标签
  private static final String USER_MAX_APPS_DEFAULT = "userMaxAppsDefault";
  // 公平份额抢占超时默认值标签
  private static final String DEFAULT_FAIR_SHARE_PREEMPTION_TIMEOUT =
      "defaultFairSharePreemptionTimeout";
  // 公平份额抢占超时标签
  private static final String FAIR_SHARE_PREEMPTION_TIMEOUT =
      "fairSharePreemptionTimeout";
  // 最小份额抢占超时默认值标签
  private static final String DEFAULT_MIN_SHARE_PREEMPTION_TIMEOUT =
      "defaultMinSharePreemptionTimeout";
  // 队列最大运行应用数默认值标签
  private static final String QUEUE_MAX_APPS_DEFAULT = "queueMaxAppsDefault";
  // 公平份额抢占阈值默认值标签
  private static final String DEFAULT_FAIR_SHARE_PREEMPTION_THRESHOLD =
      "defaultFairSharePreemptionThreshold";
  // 队列最大ApplicationMaster资源份额默认值标签
  private static final String QUEUE_MAX_AM_SHARE_DEFAULT =
      "queueMaxAMShareDefault";
  // 资源预留规划器标签
  private static final String RESERVATION_PLANNER = "reservation-planner";
  // 资源预留代理标签
  private static final String RESERVATION_AGENT = "reservation-agent";
  // 资源预留准入策略标签
  private static final String RESERVATION_ADMISSION_POLICY =
      "reservation-policy";
  // 队列放置策略标签
  private static final String QUEUE_PLACEMENT_POLICY = "queuePlacementPolicy";
  // 队列标签
  private static final String QUEUE = "queue";
  // 池标签（旧版本兼容，等价于queue）
  private static final String POOL = "pool";
  // 用户配置标签
  private static final String USER = "user";
  // 用户名属性标签
  private static final String USERNAME = "name";
  // 最大运行应用数标签
  private static final String MAX_RUNNING_APPS = "maxRunningApps";
  // 默认队列调度策略标签
  private static final String DEFAULT_QUEUE_SCHEDULING_POLICY =
      "defaultQueueSchedulingPolicy";
  // 默认队列调度模式标签（旧版本兼容，等价于defaultQueueSchedulingPolicy）
  private static final String DEFAULT_QUEUE_SCHEDULING_MODE =
      "defaultQueueSchedulingMode";

  // 存储所有合法标签名称，过滤非法配置
  private static final Set<String> VALID_TAG_NAMES =
      Sets.newHashSet(QUEUE_MAX_RESOURCES_DEFAULT, USER_MAX_APPS_DEFAULT,
          DEFAULT_FAIR_SHARE_PREEMPTION_TIMEOUT, FAIR_SHARE_PREEMPTION_TIMEOUT,
          DEFAULT_MIN_SHARE_PREEMPTION_TIMEOUT, QUEUE_MAX_APPS_DEFAULT,
          DEFAULT_FAIR_SHARE_PREEMPTION_THRESHOLD, QUEUE_MAX_AM_SHARE_DEFAULT,
          RESERVATION_PLANNER, RESERVATION_AGENT, RESERVATION_ADMISSION_POLICY,
          QUEUE_PLACEMENT_POLICY, QUEUE, POOL, USER,
          DEFAULT_QUEUE_SCHEDULING_POLICY, DEFAULT_QUEUE_SCHEDULING_MODE);

  // 待解析的DOM节点列表
  private final NodeList elements;
  // 存储全局配置标签的文本值，键为标签名
  private final Map<String, String> textValues = Maps.newHashMap();
  // 队列放置策略DOM元素
  private Element queuePlacementPolicyElement;
  // 存储所有队列配置的DOM元素
  private final List<Element> queueElements = new ArrayList<>();
  // 存储用户自定义最大运行应用数，键为用户名
  private final Map<String, Integer> userMaxApps = new HashMap<>();
  // 全局默认调度策略
  private SchedulingPolicy defaultSchedulingPolicy;

  /**
   * 构造分配文件解析器，传入待解析的DOM顶层节点列表。
   * @param elements DOM顶层节点列表
   */
  public AllocationFileParser(NodeList elements) {
    this.elements = elements;
  }

  /**
   * 遍历解析所有DOM节点，分类提取不同类型配置。
   * @throws AllocationConfigurationException 配置解析错误时抛出
   */
  public void parse() throws AllocationConfigurationException {
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        final String tagName = element.getTagName();
        if (VALID_TAG_NAMES.contains(tagName)) {
          if (tagName.equals(QUEUE_PLACEMENT_POLICY)) {
            // 保存队列放置策略元素
            queuePlacementPolicyElement = element;
          } else if (isSchedulingPolicy(element)) {
            // 解析全局默认调度策略
            defaultSchedulingPolicy = extractSchedulingPolicy(element);
          } else if (isQueue(element)) {
            // 添加队列元素到待解析列表
            queueElements.add(element);
          } else if (tagName.equals(USER)) {
            // 解析用户配置
            extractUserData(element);
          } else {
            // 保存全局配置文本值
            textValues.put(tagName, getTrimmedTextData(element));
          }
        } else {
          // 非法标签，打印警告日志
          LOG.warn("Bad element in allocations file: " + tagName);
        }
      }
    }
  }

  /**
   * 判断节点是否为默认调度策略配置。
   * @param element DOM元素
   * @return 是否为默认调度策略配置
   */
  private boolean isSchedulingPolicy(Element element) {
    return DEFAULT_QUEUE_SCHEDULING_POLICY.equals(element.getTagName())
        || DEFAULT_QUEUE_SCHEDULING_MODE.equals(element.getTagName());
  }

  /**
   * 解析用户配置元素，提取该用户的最大运行应用数限制。
   * @param element 用户DOM元素
   */
  private void extractUserData(Element element) {
    final String userName = element.getAttribute(USERNAME);
    final NodeList fields = element.getChildNodes();
    for (int j = 0; j < fields.getLength(); j++) {
      final Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      final Element field = (Element) fieldNode;
      if (MAX_RUNNING_APPS.equals(field.getTagName())) {
        final String text = getTrimmedTextData(field);
        final int val = Integer.parseInt(text);
        userMaxApps.put(userName, val);
      }
    }
  }

  /**
   * 解析并验证调度策略名称，返回对应调度策略实例。
   * @param element 调度策略DOM元素
   * @return 调度策略实例
   * @throws AllocationConfigurationException 不允许设置默认策略为FIFO时抛出
   */
  private SchedulingPolicy extractSchedulingPolicy(Element element)
      throws AllocationConfigurationException {
    String text = getTrimmedTextData(element);
    if (text.equalsIgnoreCase(FifoPolicy.NAME)) {
      throw new AllocationConfigurationException("Bad fair scheduler "
          + "config file: defaultQueueSchedulingPolicy or "
          + "defaultQueueSchedulingMode can't be FIFO.");
    }
    return SchedulingPolicy.parse(text);
  }

  /**
   * 判断节点是否为队列配置元素（兼容旧版pool标签）。
   * @param element DOM元素
   * @return 是否为队列配置元素
   */
  private boolean isQueue(Element element) {
    return element.getTagName().equals(QUEUE)
        || element.getTagName().equals(POOL);
  }

  /**
   * 获取元素文本内容并去除首尾空白。
   * @param element DOM元素
   * @return 修剪后的文本内容
   */
  private String getTrimmedTextData(Element element) {
    return ((Text) element.getFirstChild()).getData().trim();
  }

  /**
   * 获取全局默认队列最大资源限制。
   * @return 可配置资源对象
   * @throws AllocationConfigurationException 资源格式解析错误时抛出
   */
  public ConfigurableResource getQueueMaxResourcesDefault()
      throws AllocationConfigurationException {
    Optional<String> value = getTextValue(QUEUE_MAX_RESOURCES_DEFAULT);
    if (value.isPresent()) {
      return FairSchedulerConfiguration.parseResourceConfigValue(value.get());
    }
    // 未配置则返回无限制资源
    return new ConfigurableResource(Resources.unbounded());
  }

  /**
   * 获取全局默认用户最大运行应用数。
   * @return 最大应用数，未配置则返回Integer.MAX_VALUE（无限制）
   */
  public int getUserMaxAppsDefault() {
    Optional<String> value = getTextValue(USER_MAX_APPS_DEFAULT);
    return value.map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  /**
   * 获取默认公平份额抢占超时时间，单位毫秒。
   * @return 抢占超时时间，未配置则返回Long.MAX_VALUE（不抢占）
   */
  public long getDefaultFairSharePreemptionTimeout() {
    Optional<String> value = getTextValue(FAIR_SHARE_PREEMPTION_TIMEOUT);
    Optional<String> defaultValue =
        getTextValue(DEFAULT_FAIR_SHARE_PREEMPTION_TIMEOUT);

    if (value.isPresent() && !defaultValue.isPresent()) {
      // 兼容旧标签，转换为毫秒
      return Long.parseLong(value.get()) * 1000L;
    } else if (defaultValue.isPresent()) {
      // 使用默认值，转换为毫秒
      return Long.parseLong(defaultValue.get()) * 1000L;
    }
    // 未配置则不超时
    return Long.MAX_VALUE;
  }

  /**
   * 获取默认最小份额抢占超时时间，单位毫秒。
   * @return 抢占超时时间，未配置则返回Long.MAX_VALUE（不抢占）
   */
  public long getDefaultMinSharePreemptionTimeout() {
    Optional<String> value = getTextValue(DEFAULT_MIN_SHARE_PREEMPTION_TIMEOUT);
    return value.map(v -> Long.parseLong(v) * 1000L).orElse(Long.MAX_VALUE);
  }

  /**
   * 获取全局默认队列最大运行应用数。
   * @return 最大应用数，未配置则返回Integer.MAX_VALUE（无限制）
   */
  public int getQueueMaxAppsDefault() {
    Optional<String> value = getTextValue(QUEUE_MAX_APPS_DEFAULT);
    return value.map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  /**
   * 获取默认公平份额抢占阈值，范围0-1。
   * @return 抢占阈值，未配置则返回0.5f
   */
  public float getDefaultFairSharePreemptionThreshold() {
    Optional<String> value =
        getTextValue(DEFAULT_FAIR_SHARE_PREEMPTION_THRESHOLD);
    if (value.isPresent()) {
      float floatValue = Float.parseFloat(value.get());
      // 将值钳位在合法范围[0, 1]
      return Math.max(Math.min(floatValue, 1.0f), 0.0f);
    }
    return 0.5f;
  }

  /**
   * 获取全局默认队列最大ApplicationMaster资源份额，不超过1。
   * @return 最大AM资源份额，未配置则返回0.5f
   */
  public float getQueueMaxAMShareDefault() {
    Optional<String> value = getTextValue(QUEUE_MAX_AM_SHARE_DEFAULT);
    if (value.isPresent()) {
      float val = Float.parseFloat(value.get());
      // 钳位不超过1.0，避免超过整个队列资源总量
      return Math.min(val, 1.0f);
    }
    return 0.5f;
  }

  // Reservation全局配置获取方法
  /**
   * 获取资源预留规划器类名。
   * @return 规划器类名Optional
   */
  public Optional<String> getReservationPlanner() {
    return getTextValue(RESERVATION_PLANNER);
  }

  /**
   * 获取资源预留代理类名。
   * @return 代理类名Optional
   */
  public Optional<String> getReservationAgent() {
    return getTextValue(RESERVATION_AGENT);
  }

  /**
   * 获取资源预留准入策略类名。
   * @return 策略类名Optional
   */
  public Optional<String> getReservationAdmissionPolicy() {
    return getTextValue(RESERVATION_ADMISSION_POLICY);
  }

  /**
   * 获取队列放置策略DOM元素。
   * @return DOM元素Optional
   */
  public Optional<Element> getQueuePlacementPolicy() {
    return Optional.ofNullable(queuePlacementPolicyElement);
  }

  /**
   * 从textValues中获取指定配置的文本值。
   * @param key 配置标签名
   * @return 文本值Optional
   */
  private Optional<String> getTextValue(String key) {
    return Optional.ofNullable(textValues.get(key));
  }

  /**
   * 获取所有解析到的队列DOM元素列表。
   * @return 队列元素列表
   */
  public List<Element> getQueueElements() {
    return queueElements;
  }

  /**
   * 获取所有用户自定义最大运行应用数映射。
   * @return 用户->最大应用数映射
   */
  public Map<String, Integer> getUserMaxApps() {
    return userMaxApps;
  }

  /**
   * 获取全局默认调度策略。
   * @return 默认调度策略，未配置则返回调度器默认策略
   */
  public SchedulingPolicy getDefaultSchedulingPolicy() {
    if (defaultSchedulingPolicy != null) {
      return defaultSchedulingPolicy;
    }
    return SchedulingPolicy.DEFAULT_POLICY;
  }
}