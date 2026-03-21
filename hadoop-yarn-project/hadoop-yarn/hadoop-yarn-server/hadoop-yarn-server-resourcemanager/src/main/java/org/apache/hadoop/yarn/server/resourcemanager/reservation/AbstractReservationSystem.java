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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.CapacityReservationsACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.FairReservationsACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ReservationsACLsManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN ReservationSystem抽象基类，基于ResourceScheduler实现预留资源管理核心框架
 * 为不同调度器提供统一的预留资源管理基础能力
 */
@LimitedPrivate("yarn")
@Unstable
public abstract class AbstractReservationSystem extends AbstractService
    implements ReservationSystem {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractReservationSystem.class);

  // 读写锁，保证plans等共享数据的线程安全，公平模式
  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock(true);
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  // 标记是否完成初始化
  private boolean initialized = false;

  // UTC时钟，用于时间相关计算
  private final Clock clock = new UTCClock();

  //  ReservationId生成计数器，原子自增保证并发安全
  private AtomicLong resCounter = new AtomicLong();

  // 按队列名称存储所有预留计划
  private Map<String, Plan> plans = new HashMap<String, Plan>();

  // ReservationId到对应队列名称的映射，快速查找预留所属队列
  private Map<ReservationId, String> resQMap =
      new HashMap<ReservationId, String>();

  // RM上下文，获取调度器等核心组件
  private RMContext rmContext;

  // 关联的资源调度器
  private ResourceScheduler scheduler;

  // 定期执行计划同步任务的线程池
  private ScheduledExecutorService scheduledExecutorService;

  // 配置对象
  protected Configuration conf;

  // 计划同步任务执行步长（时间间隔）
  protected long planStepSize;

  // 计划同步器，负责将预留计划同步到调度器实际资源分配
  private PlanFollower planFollower;

  // 预留访问权限控制器
  private ReservationsACLsManager reservationsACLsManager;

  // 是否启用状态恢复
  private boolean isRecoveryEnabled = false;

  // 周期性预留最大允许周期
  private long maxPeriodicity;

  /**
   * 构造服务对象
   * 
   * @param name service name
   */
  public AbstractReservationSystem(String name) {
    super(name);
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    writeLock.lock();
    try {
      this.rmContext = rmContext;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException {
    writeLock.lock();
    try {
      // 未初始化则执行完整初始化
      if (!initialized) {
        initialize(conf);
        initialized = true;
      } else {
        // 已初始化则仅新增新发现的可预留队列计划
        initializeNewPlans(conf);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 完整初始化预留系统核心组件
   * @param conf 配置对象
   * @throws YarnException 初始化异常
   */
  private void initialize(Configuration conf) throws YarnException {
    LOG.info("Initializing Reservation system");
    this.conf = conf;
    scheduler = rmContext.getScheduler();
    // 从配置读取计划同步步长，使用默认值兜底
    planStepSize = conf.getTimeDuration(
        YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP,
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP,
        TimeUnit.MILLISECONDS);
    if (planStepSize < 0) {
      planStepSize =
          YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_PLAN_FOLLOWER_TIME_STEP;
    }
    // 从配置读取周期性预留最大允许周期，使用默认值兜底
    maxPeriodicity =
        conf.getLong(YarnConfiguration.RM_RESERVATION_SYSTEM_MAX_PERIODICITY,
            YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY);
    if (maxPeriodicity <= 0) {
      maxPeriodicity =
          YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY;
    }
    // 为每个可预留队列创建对应预留计划
    Set<String> planQueueNames = scheduler.getPlanQueues();
    for (String planQueueName : planQueueNames) {
      Plan plan = initializePlan(planQueueName);
      plans.put(planQueueName, plan);
    }
    // 读取恢复配置
    isRecoveryEnabled = conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

    // 如果ACL启用，根据调度器类型创建对应ACL管理器
    if (conf.getBoolean(YarnConfiguration.YARN_RESERVATION_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_RESERVATION_ACL_ENABLE)
        && conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
            YarnConfiguration.DEFAULT_YARN_ACL_ENABLE)) {
      if (scheduler instanceof CapacityScheduler) {
        reservationsACLsManager = new CapacityReservationsACLsManager(scheduler,
            conf);
      } else if (scheduler instanceof FairScheduler) {
        reservationsACLsManager = new FairReservationsACLsManager(scheduler,
            conf);
      }
    }
  }

  /**
   * 从恢复状态加载指定计划的所有预留信息
   * @param planName 计划名称
   * @param reservations 该计划所有预留的持久化状态
   * @throws PlanningException 加载恢复异常
   */
  private void loadPlan(String planName,
      Map<ReservationId, ReservationAllocationStateProto> reservations)
      throws PlanningException {
    Plan plan = plans.get(planName);
    Resource minAllocation = getMinAllocation();
    ResourceCalculator rescCalculator = getResourceCalculator();
    // 将Proto格式持久化状态转换为内存分配对象，添加到计划中
    for (Entry<ReservationId, ReservationAllocationStateProto> currentReservation : reservations
        .entrySet()) {
      plan.addReservation(ReservationSystemUtil.toInMemoryAllocation(planName,
          currentReservation.getKey(), currentReservation.getValue(),
          minAllocation, rescCalculator), true);
      resQMap.put(currentReservation.getKey(), planName);
    }
    LOG.info("Recovered reservations for Plan: {}", planName);
  }

  @Override
  public void recover(RMState state) throws Exception {
    LOG.info("Recovering Reservation system");
    writeLock.lock();
    try {
      // 从RM恢复状态获取预留系统状态
      Map<String, Map<ReservationId, ReservationAllocationStateProto>> reservationSystemState =
          state.getReservationState();
      if (planFollower != null) {
        // 逐个计划恢复预留信息
        for (String plan : plans.keySet()) {
          // 如果状态存储中存在该计划，加载恢复
          if (reservationSystemState.containsKey(plan)) {
            loadPlan(plan, reservationSystemState.get(plan));
          }
          // 同步计划到调度器
          synchronizePlan(plan, false);
        }
        // 工作保留恢复模式下，延迟启动计划同步任务
        startPlanFollower(conf.getLong(
            YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
            YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS));
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 增量初始化新增可预留队列，刷新预留系统配置
   */
  private void initializeNewPlans(Configuration conf) {
    LOG.info("Refreshing Reservation system");
    writeLock.lock();
    try {
      // 获取当前所有可预留队列
      Set<String> planQueueNames = scheduler.getPlanQueues();
      // 为新增的可预留队列创建计划
      for (String planQueueName : planQueueNames) {
        if (!plans.containsKey(planQueueName)) {
          Plan plan = initializePlan(planQueueName);
          plans.put(planQueueName, plan);
        } else {
          LOG.warn("Plan based on reservation queue {} already exists.",
              planQueueName);
        }
      }
      // 更新计划同步器的活动计划列表
      if (planFollower != null) {
        planFollower.setPlans(plans.values());
      }
    } catch (YarnException e) {
      LOG.warn("Exception while trying to refresh reservable queues", e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 根据配置创建计划同步器实例
   * @return 计划同步器实例
   */
  private PlanFollower createPlanFollower() {
    // 读取计划同步器类名，使用默认值兜底
    String planFollowerPolicyClassName =
        conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_PLAN_FOLLOWER,
            getDefaultPlanFollower());
    if (planFollowerPolicyClassName == null) {
      return null;
    }
    LOG.info("Using PlanFollowerPolicy: " + planFollowerPolicyClassName);
    try {
      // 反射加载类并实例化
      Class<?> planFollowerPolicyClazz =
          conf.getClassByName(planFollowerPolicyClassName);
      if (PlanFollower.class.isAssignableFrom(planFollowerPolicyClazz)) {
        return (PlanFollower) ReflectionUtils
            .newInstance(planFollowerPolicyClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + planFollowerPolicyClassName
            + " not instance of " + PlanFollower.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(
          "Could not instantiate PlanFollowerPolicy: "
              + planFollowerPolicyClassName,
          e);
    }
  }

  /**
   * 根据当前调度器类型获取默认计划同步器类名
   * @return 默认计划同步器全类名
   */
  private String getDefaultPlanFollower() {
    // 根据调度器类型返回对应默认实现
    if (scheduler instanceof CapacityScheduler) {
      return CapacitySchedulerPlanFollower.class.getName();
    } else if (scheduler instanceof FairScheduler) {
      return FairSchedulerPlanFollower.class.getName();
    }
    return null;
  }

  @Override
  public Plan getPlan(String planName) {
    readLock.lock();
    try {
      return plans.get(planName);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @return the planStepSize
   */
  @Override
  public long getPlanFollowerTimeStep() {
    readLock.lock();
    try {
      return planStepSize;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void synchronizePlan(String planName, boolean shouldReplan) {
    writeLock.lock();
    try {
      Plan plan = plans.get(planName);
      if (plan != null) {
        // 调用计划同步器同步单个计划
        planFollower.synchronizePlan(plan, shouldReplan);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 启动定时计划同步任务
   * @param initialDelay 首次执行延迟时间
   */
  private void startPlanFollower(long initialDelay) {
    if (planFollower != null) {
      // 创建单线程定时线程池
      scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
      // 按固定延迟启动定时任务
      scheduledExecutorService.scheduleWithFixedDelay(planFollower,
          initialDelay, planStepSize, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    // 执行初始化/重初始化
    reinitialize(configuration, rmContext);
    // 创建计划同步器
    planFollower = createPlanFollower();
    // 初始化计划同步器
    if (planFollower != null) {
      planFollower.init(clock, scheduler, plans.values());
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    // 未启用恢复则直接启动计划同步，启用恢复则由恢复流程触发启动
    if (!isRecoveryEnabled) {
      startPlanFollower(planStepSize);
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() {
    // 关闭定时线程池
    if (scheduledExecutorService != null
        && !scheduledExecutorService.isShutdown()) {
      scheduledExecutorService.shutdown();
    }
    // 清空计划缓存
    plans.clear();
  }

  @Override
  public String getQueueForReservation(ReservationId reservationId) {
    readLock.lock();
    try {
      return resQMap.get(reservationId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setQueueForReservation(ReservationId reservationId,
      String queueName) {
    writeLock.lock();
    try {
      resQMap.put(reservationId, queueName);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public ReservationId getNewReservationId() {
    writeLock.lock();
    try {
      // 使用集群时间戳+原子计数器生成唯一ReservationId
      ReservationId resId = ReservationId.newInstance(
          ResourceManager.getClusterTimeStamp(), resCounter.incrementAndGet());
      LOG.info("Allocated new reservationId: " + resId);
      return resId;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Map<String, Plan> getAllPlans() {
    return plans;
  }

  /**
   * 根据调度器类型获取对应默认预留系统实现类名
   * 
   * @param scheduler the scheduler for which the reservation system is required
   *
   * @return the {@link ReservationSystem} based on the configured scheduler
   */
  public static String getDefaultReservationSystem(
      ResourceScheduler scheduler) {
    if (scheduler instanceof CapacityScheduler) {
      return CapacityReservationSystem.class.getName();
    } else if (scheduler instanceof FairScheduler) {
      return FairReservationSystem.class.getName();
    }
    return null