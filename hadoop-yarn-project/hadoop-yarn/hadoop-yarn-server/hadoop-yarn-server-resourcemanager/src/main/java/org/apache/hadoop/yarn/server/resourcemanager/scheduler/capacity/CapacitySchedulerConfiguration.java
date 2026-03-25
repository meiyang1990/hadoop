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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.QueueCapacityConfigParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.MappingRuleCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLConfigurationParser.AppPriorityACLKeyType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.WorkflowPriorityMappingsManager.WorkflowPriorityMapping;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeLookupPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodePolicySpec;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyForPendingApps;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyWithExclusivePartitions;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getAutoCreatedQueueObjectTemplateConfPrefix;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getNodeLabelPrefix;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

/**
 * 容量调度器配置类，继承预约调度配置，提供容量调度器所有配置项的读取和管理能力
 */
public class CapacitySchedulerConfiguration extends ReservationSchedulerConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerConfiguration.class);

  /** 容量调度器配置文件名 */
  private static final String CS_CONFIGURATION_FILE = "capacity-scheduler.xml";

  @Private
  public static final String PREFIX = "yarn.scheduler.capacity.";

  @Private
  public static final String DOT = ".";

  @Private
  public static final String MAXIMUM_APPLICATIONS_SUFFIX =
    "maximum-applications";

  @Private
  public static final String MAXIMUM_SYSTEM_APPLICATIONS =
    PREFIX + MAXIMUM_APPLICATIONS_SUFFIX;

  @Private
  public static final String MAXIMUM_AM_RESOURCE_SUFFIX =
    "maximum-am-resource-percent";

  @Private
  public static final String MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT =
    PREFIX + MAXIMUM_AM_RESOURCE_SUFFIX;

  @Private
  public static final String QUEUES = "queues";

  @Private
  public static final String CAPACITY = "capacity";

  @Private
  public static final String MAXIMUM_CAPACITY = "maximum-capacity";

  @Private
  public static final String USER_LIMIT = "minimum-user-limit-percent";

  @Private
  public static final String USER_LIMIT_FACTOR = "user-limit-factor";

  @Private
  public static final String USER_WEIGHT = "weight";

  @Private
  public static final String USER_SETTINGS = "user-settings";

  @Private
  public static final String USER_WEIGHT_REGEX = "\\S+\\." + USER_WEIGHT;

  @Private
  public static final Pattern USER_WEIGHT_PATTERN = Pattern.compile(
      USER_WEIGHT_REGEX);

  @Private
  public static final float DEFAULT_USER_WEIGHT = 1.0f;

  @Private
  public static final String STATE = "state";

  @Private
  public static final String ACCESSIBLE_NODE_LABELS = "accessible-node-labels";

  @Private
  public static final String DEFAULT_NODE_LABEL_EXPRESSION =
      "default-node-label-expression";

  public static final String RESERVE_CONT_LOOK_ALL_NODES = PREFIX
      + "reservations-continue-look-all-nodes";

  @Private
  public static final boolean DEFAULT_RESERVE_CONT_LOOK_ALL_NODES = true;

  public static final String SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS = PREFIX
      + "skip-allocate-on-nodes-with-reserved-containers";

  @Private
  public static final boolean DEFAULT_SKIP_ALLOCATE_ON_NODES_WITH_RESERVED_CONTAINERS = false;

  @Private
  public static final String MAXIMUM_ALLOCATION = "maximum-allocation";

  @Private
  public static final String MAXIMUM_ALLOCATION_MB = "maximum-allocation-mb";

  @Private
  public static final String MAXIMUM_ALLOCATION_VCORES =
          "maximum-allocation-vcores";
  /**
   * Ordering policy of queues
   */
  public static final String ORDERING_POLICY = "ordering-policy";

  /*
   * Ordering policy inside a leaf queue to sort apps
   */
  public static final String FIFO_APP_ORDERING_POLICY = "fifo";

  public static final String FAIR_APP_ORDERING_POLICY = "fair";

  public static final String FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY
      = "fifo-with-partitions";

  public static final String FIFO_FOR_PENDING_APPS
      = "fifo-for-pending-apps";

  public static final String DEFAULT_APP_ORDERING_POLICY =
      FIFO_APP_ORDERING_POLICY;

  @Private
  public static final int DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS = 10000;

  @Private
  public static final float
  DEFAULT_MAXIMUM_APPLICATIONMASTERS_RESOURCE_PERCENT = 0.1f;

  @Private
  public static final float UNDEFINED = -1;

  @Private
  public static final float MINIMUM_CAPACITY_VALUE = 0;

  @Private
  public static final float MAXIMUM_CAPACITY_VALUE = 100;

  @Private
  public static final float DEFAULT_MAXIMUM_CAPACITY_VALUE = -1.0f;

  @Private
  public static final int DEFAULT_USER_LIMIT = 100;

  @Private
  public static final float DEFAULT_USER_LIMIT_FACTOR = 1.0f;

  @Private
  public static final String ALL_ACL = "*";

  @Private
  public static final String NONE_ACL = " ";

  @Private public static final String ENABLE_USER_METRICS =
      PREFIX +"user-metrics.enable";
  @Private public static final boolean DEFAULT_ENABLE_USER_METRICS = false;

  /** ResourceComparator for scheduling. */
  @Private public static final String RESOURCE_CALCULATOR_CLASS =
      PREFIX + "resource-calculator";

  @Private public static final Class<? extends ResourceCalculator>
  DEFAULT_RESOURCE_CALCULATOR_CLASS = DefaultResourceCalculator.class;

  @Private
  public static final String ROOT = "root";

  @Private
  public static final String NODE_LOCALITY_DELAY =
     PREFIX + "node-locality-delay";

  @Private
  public static final int DEFAULT_NODE_LOCALITY_DELAY = 40;

  @Private
  public static final String RACK_LOCALITY_ADDITIONAL_DELAY =
          PREFIX + "rack-locality-additional-delay";

  @Private
  public static final int DEFAULT_RACK_LOCALITY_ADDITIONAL_DELAY = -1;

  @Private
  public static final String RACK_LOCALITY_FULL_RESET =
      PREFIX + "rack-locality-full-reset";

  @Private
  public static final int DEFAULT_OFFSWITCH_PER_HEARTBEAT_LIMIT = 1;

  @Private
  public static final String OFFSWITCH_PER_HEARTBEAT_LIMIT =
      PREFIX + "per-node-heartbeat.maximum-offswitch-assignments";

  @Private
  public static final boolean DEFAULT_RACK_LOCALITY_FULL_RESET = true;

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_PREFIX =
      PREFIX + "schedule-asynchronously";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_ENABLE =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".enable";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".maximum-threads";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_PENDING_BACKLOGS =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".maximum-pending-backlogs";

  @Private
  public static final String SCHEDULE_ASYNCHRONOUSLY_INTERVAL =
      SCHEDULE_ASYNCHRONOUSLY_PREFIX + ".scheduling-interval-ms";
  @Private
  public static final long DEFAULT_SCHEDULE_ASYNCHRONOUSLY_INTERVAL = 5;

  @Private
  public static final String APP_FAIL_FAST = PREFIX + "application.fail-fast";

  @Private
  public static final boolean DEFAULT_APP_FAIL_FAST = false;

  @Private
  public static final Integer
      DEFAULT_SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_PENDING_BACKLOGS = 100;

  @Private
  public static final boolean DEFAULT_SCHEDULE_ASYNCHRONOUSLY_ENABLE = true;

  @Private
  public static final String QUEUE_MAPPING = PREFIX + "queue-mappings";

  @Private
  public static final String QUEUE_MAPPING_NAME =
      YarnConfiguration.QUEUE_PLACEMENT_RULES + ".app-name";

  @Private
  public static final String ENABLE_QUEUE_MAPPING_OVERRIDE = QUEUE_MAPPING + "-override.enable";

  @Private
  public static final boolean DEFAULT_ENABLE_QUEUE_MAPPING_OVERRIDE = false;

  @Private
  public static final String WORKFLOW_PRIORITY_MAPPINGS =
      PREFIX + "workflow-priority-mappings";

  @Private
  public static final String ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE =
      WORKFLOW_PRIORITY_MAPPINGS + "-override.enable";

  @Private
  public static final boolean DEFAULT_ENABLE_WORKFLOW_PRIORITY_MAPPINGS_OVERRIDE = false;

  @Private
  public static final String QUEUE_PREEMPTION_DISABLED = "disable_preemption";

  @Private
  public static final String DEFAULT_APPLICATION_PRIORITY = "default-application-priority";

  @Private
  public static final Integer DEFAULT_CONFIGURATION_APPLICATION_PRIORITY = 0;

  @Private
  public static final String AVERAGE_CAPACITY = "average-capacity";

  @Private
  public static final String IS_RESERVABLE = "reservable";

  @Private
  public static final String RESERVATION_WINDOW = "reservation-window";

  @Private
  public static final String INSTANTANEOUS_MAX_CAPACITY =
      "instantaneous-max-capacity";

  @Private
  public static final String RESERVATION_ADMISSION_POLICY =
      "reservation-policy";

  @Private
  public static final String RESERVATION_AGENT_NAME = "reservation-agent";

  @Private
  public static final String RESERVATION_SHOW_RESERVATION_AS_QUEUE =
      "show-reservations-as-queues";

  @Private
  public static final String RESERVATION_PLANNER_NAME = "reservation-planner";

  @Private
  public static final String RESERVATION_MOVE_ON_EXPIRY =
      "reservation-move-on-expiry";

  @Private
  public static final String RESERVATION_ENFORCEMENT_WINDOW =
      "reservation-enforcement-window";

  @Private
  public static final String LAZY_PREEMPTION_ENABLED =
      PREFIX + "lazy-preemption-enabled";

  @Private
  public static final boolean DEFAULT_LAZY_PREEMPTION_ENABLED = false;

  @Private
  public static final String ASSIGN_MULTIPLE_ENABLED = PREFIX
      + "per-node-heartbeat.multiple-assignments-enabled";

  @Private
  public static final boolean DEFAULT_ASSIGN_MULTIPLE_ENABLED = true;

  /** Maximum number of containers to assign on each check-in. */
  @Private
  public static final String MAX_ASSIGN_PER_HEARTBEAT = PREFIX
      + "per-node-heartbeat.maximum-container-assignments";

  /**
   * Avoid potential risk that greedy assign multiple may involve
   * */
  @Private
  public static final int DEFAULT_MAX_ASSIGN_PER_HEARTBEAT = 100;

  /** Configuring absolute min/max resources in a queue. **/
  @Private
  public static final String MINIMUM_RESOURCE = "min-resource";

  @Private
  public static final String MAXIMUM_RESOURCE = "max-resource";

  public static final String DEFAULT_RESOURCE_TYPES = "memory,vcores";

  public static final String PATTERN_FOR_ABSOLUTE_RESOURCE = "^\\[[\\w\\.,\\-_=\\ /]+\\]$";

  public static final Pattern RESOURCE_PATTERN = Pattern.compile(PATTERN_FOR_ABSOLUTE_RESOURCE);

  private static final String WEIGHT_SUFFIX = "w";

  public static final String MAX_PARALLEL_APPLICATIONS = "max-parallel-apps";

  public static final int DEFAULT_MAX_PARALLEL_APPLICATIONS = Integer.MAX_VALUE;

  public static final String ALLOW_ZERO_CAPACITY_SUM =
      "allow-zero-capacity-sum";

  public static final boolean DEFAULT_ALLOW_ZERO_CAPACITY_SUM = false;
  public static final String MAPPING_RULE_FORMAT =
      PREFIX + "mapping-rule-format";
  public static final String MAPPING_RULE_JSON =
      PREFIX + "mapping-rule-json";
  public static final String MAPPING_RULE_JSON_FILE =
      PREFIX + "mapping-rule-json-file";

  public static final String MAPPING_RULE_FORMAT_LEGACY = "legacy";
  public static final String MAPPING_RULE_FORMAT_JSON = "json";

  public static final String MAPPING_RULE_FORMAT_DEFAULT =
      MAPPING_RULE_FORMAT_LEGACY;

  private static final QueueCapacityConfigParser queueCapacityConfigParser
      = new QueueCapacityConfigParser();
  private static final String LEGACY_QUEUE_MODE_ENABLED = PREFIX + "legacy-queue-mode.enabled";
  public static final boolean DEFAULT_LEGACY_QUEUE_MODE = true;

  /** 缓存解析后的配置属性 */
  private ConfigurationProperties configurationProperties