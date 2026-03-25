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

package org.apache.hadoop.yarn.server.nodemanager.health;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * NodeManager节点健康检查聚合服务，负责管理多个健康检查器，汇总所有检查结果输出节点整体健康状态。
 * 继承自CompositeService，所有注册的服务必须实现HealthReporter接口，否则初始化会抛出异常。
 * 所有已注册健康检查器的报告将被聚合，统一对外提供节点健康状态。
 * 
 * @see HealthReporter
 * @see LocalDirsHandlerService
 * @see TimedHealthReporterService
 */
public class NodeHealthCheckerService extends CompositeService
    implements HealthReporter {

  public static final Logger LOG =
      LoggerFactory.getLogger(NodeHealthCheckerService.class);
  // 最大允许同时运行的健康检查脚本数量，限制避免性能问题
  private static final int MAX_SCRIPTS = 4;

  // 所有已注册的健康检查器列表
  private List<HealthReporter> reporters;
  // 本地磁盘目录健康检查器引用
  private LocalDirsHandlerService dirsHandler;
  // 异常报告器，用于接收并上报节点异常
  private ExceptionReporter exceptionReporter;

  // 健康报告分隔符
  public static final String SEPARATOR = ";";

  /**
   * 构造节点健康检查服务，传入磁盘目录检查器。
   * @param dirHandlerService 本地磁盘目录健康检查器
   */
  public NodeHealthCheckerService(
      LocalDirsHandlerService dirHandlerService) {
    super(NodeHealthCheckerService.class.getName());

    this.reporters = new ArrayList<>();
    this.dirsHandler = dirHandlerService;
    this.exceptionReporter = new ExceptionReporter();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 添加异常报告器到检查列表
    reporters.add(exceptionReporter);
    // 添加磁盘目录健康检查器
    addHealthReporter(dirsHandler);
    // 从配置中读取自定义健康检查脚本配置
    String[] configuredScripts = conf.getTrimmedStrings(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPTS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_SCRIPTS);
    // 检查脚本数量不超过上限，避免性能问题
    if (configuredScripts.length > MAX_SCRIPTS) {
      throw new IllegalArgumentException("Due to performance reasons " +
          "running more than " + MAX_SCRIPTS + "scripts is not allowed.");
    }
    // 遍历创建并添加每个自定义健康检查脚本Runner
    for (String configuredScript : configuredScripts) {
      addHealthReporter(NodeHealthScriptRunner.newInstance(
          configuredScript, conf));
    }
    super.serviceInit(conf);
  }

  /**
   * 添加一个实现了HealthReporter接口的服务到健康检查列表，若服务已存在则跳过。
   * @param service 要添加的健康检查服务
   * @throws Exception 如果提供的服务未实现HealthReporter接口则抛出异常
   */
  @VisibleForTesting
  void addHealthReporter(Service service) throws Exception {
    if (service != null) {
      // 检查是否已存在同名服务，避免重复添加
      if (getServices().stream()
          .noneMatch(x -> x.getName().equals(service.getName()))) {
        // 验证服务必须实现HealthReporter接口
        if (!(service instanceof HealthReporter)) {
          throw new Exception("Attempted to add service to " +
              "NodeHealthCheckerService that does not implement " +
              "HealthReporter.");
        }
        // 添加到健康检查器列表和CompositeService管理
        reporters.add((HealthReporter) service);
        addService(service);
      } else {
        LOG.debug("Omitting duplicate service: {}.", service.getName());
      }
    }
  }

  /**
   * 聚合所有健康检查器的报告，拼接成完整的节点健康报告。
   * @return 拼接后的节点健康报告字符串
   */
  @Override
  public String getHealthReport() {
    // 收集所有非空健康报告
    ArrayList<String> reports = reporters.stream()
        .map(reporter -> Strings.emptyToNull(reporter.getHealthReport()))
        .collect(Collectors.toCollection(ArrayList::new));
    // 用分隔符拼接所有报告，跳过空值
    return Joiner.on(SEPARATOR).skipNulls().join(reports);
  }

  /**
   * 检查节点整体是否健康，所有检查器都健康才返回健康。
   * @return true 节点健康，false 任一检查器判定节点不健康
   */
  @Override
  public boolean isHealthy() {
    return reporters.stream().allMatch(HealthReporter::isHealthy);
  }

  /**
   * 获取最新的健康报告时间，取所有检查器中最新的时间。
   * @return 最后一次健康报告的时间戳
   */
  @Override
  public long getLastHealthReportTime() {
    Optional<Long> max = reporters.stream()
        .map(HealthReporter::getLastHealthReportTime).max(Long::compareTo);
    return max.orElse(0L);
  }

  /**
   * 获取磁盘健康检查器实例。
   * @return 磁盘目录处理器实例
   */
  public LocalDirsHandlerService getDiskHandler() {
    return dirsHandler;
  }

  /**
   * 上报节点异常到异常报告器，纳入健康检查结果。
   * @param exception 需要上报的节点异常
   */
  public void reportException(Exception exception) {
    exceptionReporter.reportException(exception);
  }
}