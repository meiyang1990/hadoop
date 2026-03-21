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
package org.apache.hadoop.yarn.server.sharedcachemanager.metrics;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 维护YARN共享缓存上传器的请求指标，通过Hadoop metrics2接口对外发布
 */
@Private
@Evolving
@Metrics(about="shared cache upload metrics", context="yarn")
public class SharedCacheUploaderMetrics {

  static final Logger LOG =
      LoggerFactory.getLogger(SharedCacheUploaderMetrics.class);
  final MetricsRegistry registry;
  private final static SharedCacheUploaderMetrics INSTANCE = create();

  private SharedCacheUploaderMetrics() {
    registry = new MetricsRegistry("SharedCacheUploaderRequests");
    LOG.debug("Initialized {}", registry);
  }

  /** 获取单例指标实例 */
  public static SharedCacheUploaderMetrics getInstance() {
    return INSTANCE;
  }

  /** 创建并注册指标实例到默认指标系统 */
  static SharedCacheUploaderMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();

    SharedCacheUploaderMetrics metrics =
        new SharedCacheUploaderMetrics();
    ms.register("SharedCacheUploaderRequests", null, metrics);
    return metrics;
  }

  @Metric("Number of accepted uploads") MutableCounterLong acceptedUploads;
  @Metric("Number of rejected uploads") MutableCounterLong rejectedUploads;

  /** 增加一个已接受上传的计数 */
  public void incAcceptedUploads() {
    acceptedUploads.incr();
  }

  /** 增加一个已拒绝上传的计数 */
  public void incRejectedUploads() {
    rejectedUploads.incr();
  }

  /** 获取已接受上传总数量 */
  public long getAcceptedUploads() { return acceptedUploads.value(); }
  /** 获取已拒绝上传总数量 */
  public long getRejectUploads() { return rejectedUploads.value(); }
}