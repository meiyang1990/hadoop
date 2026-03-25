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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;

import org.apache.hadoop.thirdparty.com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * YARN RM指标不变量检查器，通过文件中定义的JavaScript布尔表达式，
 * 定期检查队列指标和JVM指标是否满足预期不变量约束。
 * 可配置违反不变量时抛出异常或仅打日志告警，复杂不变量会增加检查开销。
 */
public class MetricsInvariantChecker extends InvariantsChecker {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsInvariantChecker.class);
  /** 不变量配置文件路径配置项key */
  public static final String INVARIANTS_FILE =
      "yarn.resourcemanager.invariant-checker.file";

  private MetricsSystem metricsSystem;
  private MetricsCollectorImpl collector;
  private SimpleBindings bindings;
  private ScriptEngineManager manager;
  private Compilable scriptEngine;
  private String invariantFile;
  /** 存储预编译后的单个不变量表达式，key为原表达式字符串 */
  private Map<String, CompiledScript> invariants;
  /** 所有不变量合并后的预编译表达式，用于快速批量检查 */
  private CompiledScript combinedInvariants;

  // 被监控的指标对象
  private QueueMetrics queueMetrics;
  private JvmMetrics jvmMetrics;

  @Override
  public void init(Configuration config, RMContext rmContext,
      ResourceScheduler scheduler) {

    super.init(config, rmContext, scheduler);

    // 获取默认指标系统实例
    this.metricsSystem = DefaultMetricsSystem.instance();
    // 获取根队列指标对象
    this.queueMetrics =
        QueueMetrics.forQueue(metricsSystem, "root", null, false, getConf());
    // 获取JVM指标对象
    this.jvmMetrics = (JvmMetrics) metricsSystem.getSource("JvmMetrics");

    // 初始化指标收集器，首次收集所有指标
    collector = new MetricsCollectorImpl();
    queueMetrics.getMetrics(collector, true);
    jvmMetrics.getMetrics(collector, true);

    // 初始化脚本引擎和绑定变量容器
    this.bindings = new SimpleBindings();
    this.manager = new ScriptEngineManager();
    this.scriptEngine = (Compilable) manager.getEngineByName("JavaScript");

    // 从配置读取不变量文件路径
    this.invariantFile = getConf().get(MetricsInvariantChecker.INVARIANTS_FILE);

    this.invariants = new HashMap<>();

    // 预加载所有指标到绑定变量，替换空格为下划线符合标识符规范
    queueMetrics.getMetrics(collector, true);
    jvmMetrics.getMetrics(collector, true);
    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric am : record.metrics()) {
        bindings.put(am.name().replace(' ', '_'), am.value());
      }
    }

    StringBuilder sb = new StringBuilder();
    try {
      // 按行读取不变量文件中所有表达式
      List<String> tempInv =
          Files.readLines(new File(invariantFile), StandardCharsets.UTF_8);


      boolean first = true;
      // 预编译每个独立不变量表达式
      for (String inv : tempInv) {

        if(first) {
          first = false;
        } else {
          // 用逻辑与连接所有表达式，构建合并表达式
          sb.append("&&");
        }

        invariants.put(inv, scriptEngine.compile(inv));
        sb.append(" (")
            .append(inv)
            .append(") ");
      }

      // 编译合并后的表达式，用于后续快速批量检查
      combinedInvariants = scriptEngine.compile(sb.toString());

    } catch (IOException e) {
      throw new RuntimeException(
          "Error loading invariant file: " + e.getMessage());
    } catch (ScriptException e) {
      throw new RuntimeException("Error compiling invariant " + e.getMessage());
    }

  }

  @Override
  public void editSchedule() {
    // 清空收集器，收集本次调度变更后变化的指标
    collector.clear();
    queueMetrics.getMetrics(collector, false);
    jvmMetrics.getMetrics(collector, false);

    // 更新绑定变量中所有变化指标的最新值
    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric am : record.metrics()) {
        bindings.put(am.name().replace(' ', '_'), am.value());
      }
    }

    // 执行所有不变量检查
    try {

      // 优先使用合并表达式快速批量检查所有不变量
      boolean allInvHold = (boolean) combinedInvariants.eval(bindings);

      // 如果有不变量不满足，逐个检查定位具体哪个不满足
      if (!allInvHold) {
        for (Map.Entry<String, CompiledScript> e : invariants.entrySet()) {
          boolean invariantsHold = (boolean) e.getValue().eval(bindings);
          if (!invariantsHold) {
            // 提取该不变量用到的所有指标变量，精简日志输出
            Map<String, Object> matchingBindings =
                extractMatchingBindings(e.getKey(), bindings);
            logOrThrow("Invariant \"" + e.getKey()
                + "\" is NOT holding, with bindings: " + matchingBindings);
          }
        }
      }
    } catch (ScriptException e) {
      logOrThrow(e.getMessage());
    }
  }

  /**
   * 提取表达式中用到的所有指标变量及其当前值，用于精简错误日志
   * @param inv 不变量表达式字符串
   * @param allBindings 所有绑定的指标变量
   * @return 仅包含当前表达式用到的变量映射
   */
  private static Map<String, Object> extractMatchingBindings(String inv,
      SimpleBindings allBindings) {
    Map<String, Object> matchingBindings = new HashMap<>();
    for (Map.Entry<String, Object> s : allBindings.entrySet()) {
      if (inv.contains(s.getKey())) {
        matchingBindings.put(s.getKey(), s.getValue());
      }
    }
    return matchingBindings;
  }
}