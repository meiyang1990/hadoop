// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * 时间线数据管理器的指标收集类，用于统计各个数据操作接口的调用次数、处理量和处理耗时。
 * 为YARN应用历史服务提供可监控的运行指标。
 */
@Metrics(about="Metrics for TimelineDataManager", context="yarn")
public class TimelineDataManagerMetrics {
  @Metric("getEntities calls")
  MutableCounterLong getEntitiesOps;

  @Metric("Entities returned via getEntities")
  MutableCounterLong getEntitiesTotal;

  @Metric("getEntities processing time")
  MutableRate getEntitiesTime;

  @Metric("getEntity calls")
  MutableCounterLong getEntityOps;

  @Metric("getEntity processing time")
  MutableRate getEntityTime;

  @Metric("getEvents calls")
  MutableCounterLong getEventsOps;

  @Metric("Events returned via getEvents")
  MutableCounterLong getEventsTotal;

  @Metric("getEvents processing time")
  MutableRate getEventsTime;

  @Metric("postEntities calls")
  MutableCounterLong postEntitiesOps;

  @Metric("Entities posted via postEntities")
  MutableCounterLong postEntitiesTotal;

  @Metric("postEntities processing time")
  MutableRate postEntitiesTime;

  @Metric("putDomain calls")
  MutableCounterLong putDomainOps;

  @Metric("putDomain processing time")
  MutableRate putDomainTime;

  @Metric("getDomain calls")
  MutableCounterLong getDomainOps;

  @Metric("getDomain processing time")
  MutableRate getDomainTime;

  @Metric("getDomains calls")
  MutableCounterLong getDomainsOps;

  @Metric("Domains returned via getDomains")
  MutableCounterLong getDomainsTotal;

  @Metric("getDomains processing time")
  MutableRate getDomainsTime;

  /**
   * 统计所有接口总调用次数。
   * @return 所有操作接口的累计调用次数
   */
  @Metric("Total calls")
  public long totalOps() {
    return getEntitiesOps.value() +
        getEntityOps.value() +
        getEventsOps.value() +
        postEntitiesOps.value() +
        putDomainOps.value() +
        getDomainOps.value() +
        getDomainsOps.value();
  }

  private static TimelineDataManagerMetrics instance = null;

  TimelineDataManagerMetrics() {
  }

  /**
   * 单例模式创建并注册指标对象到默认指标系统。
   * @return 单例的TimelineDataManagerMetrics实例
   */
  public static synchronized TimelineDataManagerMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(new TimelineDataManagerMetrics());
    }
    return instance;
  }

  /** getEntities调用次数自增 */
  public void incrGetEntitiesOps() {
    getEntitiesOps.incr();
  }

  /** 累加getEntities返回的实体总数 */
  public void incrGetEntitiesTotal(long delta) {
    getEntitiesTotal.incr(delta);
  }

  /** 添加getEntities单次处理耗时 */
  public void addGetEntitiesTime(long msec) {
    getEntitiesTime.add(msec);
  }

  /** getEntity调用次数自增 */
  public void incrGetEntityOps() {
    getEntityOps.incr();
  }

  /** 添加getEntity单次处理耗时 */
  public void addGetEntityTime(long msec) {
    getEntityTime.add(msec);
  }

  /** getEvents调用次数自增 */
  public void incrGetEventsOps() {
    getEventsOps.incr();
  }

  /** 累加getEvents返回的事件总数 */
  public void incrGetEventsTotal(long delta) {
    getEventsTotal.incr(delta);
  }

  /** 添加getEvents单次处理耗时 */
  public void addGetEventsTime(long msec) {
    getEventsTime.add(msec);
  }

  /** postEntities调用次数自增 */
  public void incrPostEntitiesOps() {
    postEntitiesOps.incr();
  }

  /** 累加postEntities写入的实体总数 */
  public void incrPostEntitiesTotal(long delta) {
    postEntitiesTotal.incr(delta);
  }

  /** 添加postEntities单次处理耗时 */
  public void addPostEntitiesTime(long msec) {
    postEntitiesTime.add(msec);
  }

  /** putDomain调用次数自增 */
  public void incrPutDomainOps() {
    putDomainOps.incr();
  }

  /** 添加putDomain单次处理耗时 */
  public void addPutDomainTime(long msec) {
    putDomainTime.add(msec);
  }

  /** getDomain调用次数自增 */
  public void incrGetDomainOps() {
    getDomainOps.incr();
  }

  /** 添加getDomain单次处理耗时 */
  public void addGetDomainTime(long msec) {
    getDomainTime.add(msec);
  }

  /** getDomains调用次数自增 */
  public void incrGetDomainsOps() {
    getDomainsOps.incr();
  }

  /** 累加getDomains返回的域总数 */
  public void incrGetDomainsTotal(long delta) {
    getDomainsTotal.incr(delta);
  }

  /** 添加getDomains单次处理耗时 */
  public void addGetDomainsTime(long msec) {
    getDomainsTime.add(msec);
  }
}