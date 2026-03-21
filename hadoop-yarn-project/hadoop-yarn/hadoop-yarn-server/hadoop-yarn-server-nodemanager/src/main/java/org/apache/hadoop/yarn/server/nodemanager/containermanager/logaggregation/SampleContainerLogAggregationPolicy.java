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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.ContainerLogAggregationPolicy;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;

/**
 * 示例容器日志聚合采样策略，对成功执行的工作容器日志进行采样聚合。
 * AM容器、失败/被杀死的工作容器始终聚合日志。为保证小应用保留足够日志，
 * 仅对超过最小容器数量的部分进行采样。可通过SAMPLE_RATE（采样率）和
 * MIN_THRESHOLD（最小保留容器数）配置。例如采样率0.2，最小阈值20，
 * 应用有100个成功工作容器时，将聚合 20 + (100-20) * 0.2 = 36 个容器日志。
 */
@Private
public class SampleContainerLogAggregationPolicy implements
    ContainerLogAggregationPolicy  {
  private static final Logger LOG =
      LoggerFactory.getLogger(SampleContainerLogAggregationPolicy.class);

  static String SAMPLE_RATE = "SR";
  public static final float DEFAULT_SAMPLE_RATE = 0.2f;

  static String MIN_THRESHOLD = "MIN";
  public static final int DEFAULT_SAMPLE_MIN_THRESHOLD = 20;

  private float sampleRate = DEFAULT_SAMPLE_RATE;
  private int minThreshold = DEFAULT_SAMPLE_MIN_THRESHOLD;

  /**
   * 构建策略参数字符串。
   * @param sampleRate 采样率
   * @param minThreshold 最小保留容器数阈值
   * @return 格式化后的参数字符串
   */
  static public String buildParameters(float sampleRate, int minThreshold) {
    StringBuilder sb = new StringBuilder();
    sb.append(SAMPLE_RATE).append(":").append(sampleRate).append(",").
        append(MIN_THRESHOLD).append(":").append(minThreshold);
    return sb.toString();
  }

  /**
   * 解析配置参数字符串，参数为逗号分隔的键值对，例如 "SR:0.5,MIN:50"
   * @param parameters 输入参数字符串
   */
  public void parseParameters(String parameters) {
    Collection<String> params = StringUtils.getStringCollection(parameters);
    for(String param : params) {
      // 拆分键值对，第一个元素为属性名，第二个为属性值
      String[] property = StringUtils.getStrings(param, ":");
      if (property == null || property.length != 2) {
        continue;
      }
      if (property[0].equals(SAMPLE_RATE)) {
        try {
          float sampleRate = Float.parseFloat(property[1]);
          // 校验采样率范围在[0, 1]之间
          if (sampleRate >= 0.0 && sampleRate <= 1.0) {
            this.sampleRate = sampleRate;
          } else {
            LOG.warn("The format isn't valid. Sample rate falls back to the " +
                "default value " + DEFAULT_SAMPLE_RATE);
          }
        } catch (NumberFormatException nfe) {
          LOG.warn("The format isn't valid. Sample rate falls back to the " +
              "default value " + DEFAULT_SAMPLE_RATE);
        }
      } else if (property[0].equals(MIN_THRESHOLD)) {
        try {
          int minThreshold = Integer.parseInt(property[1]);
          // 校验最小阈值非负
          if (minThreshold >= 0) {
            this.minThreshold = minThreshold;
          } else {
            LOG.warn("The format isn't valid. Min threshold falls back to " +
                "the default value " + DEFAULT_SAMPLE_MIN_THRESHOLD);
          }
        } catch (NumberFormatException nfe) {
          LOG.warn("The format isn't valid. Min threshold falls back to the " +
              "default value " + DEFAULT_SAMPLE_MIN_THRESHOLD);
        }
      }
    }
  }

  /**
   * 判断当前容器是否需要进行日志聚合。
   * @param logContext 容器日志上下文信息
   * @return true表示需要聚合，false表示跳过
   */
  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    if (logContext.getContainerType() ==
        ContainerType.APPLICATION_MASTER || logContext.getExitCode() != 0) {
      // AM容器、失败/被杀死容器始终聚合日志
      return true;
    }

    // 仅对大应用执行日志采样，保证小应用保留所有日志
    // 假设容器ID从1开始连续分配，工作容器从ID=2开始编号
    // 因此ID在[2, minThreshold + 1]范围内的工作容器都保留聚合
    if ((logContext.getContainerId().getContainerId() &
        ContainerId.CONTAINER_ID_BITMASK) < minThreshold + 2) {
      return true;
    }

    // 对超过阈值的成功工作容器按采样率进行采样
    return (sampleRate != 0 &&
        logContext.getContainerId().hashCode() % (int)(1/sampleRate) == 0);
  }
}