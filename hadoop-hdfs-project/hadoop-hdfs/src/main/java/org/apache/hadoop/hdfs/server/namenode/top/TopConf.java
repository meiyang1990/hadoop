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
package org.apache.hadoop.hdfs.server.namenode.top;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Preconditions;

/**
 * NNTop 功能配置类，统一管理NameNode拓扑监控功能的所有配置参数。
 * 负责从配置文件加载配置并进行合法性校验，为后续指标统计提供配置基础。
 */
@InterfaceAudience.Private
public final class TopConf {
  /**
   * 是否开启NNTop指标监控功能
   */
  public final boolean isEnabled;

  /**
   * 聚合所有命令的元命令标识，代表所有调用的总次数
   */
  public static final String ALL_CMDS = "*";

  /**
   * NNTop各统计周期的长度，单位为毫秒
   */
  public final int[] nntopReportingPeriodsMs;

  /**
   * 从配置对象加载并校验NNTop功能配置，构造配置实例
   * @param conf Hadoop配置对象
   */
  public TopConf(Configuration conf) {
    isEnabled = conf.getBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY,
        DFSConfigKeys.NNTOP_ENABLED_DEFAULT);
    String[] periodsStr = conf.getTrimmedStrings(
        DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY,
        DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT);
    nntopReportingPeriodsMs = new int[periodsStr.length];
    // 遍历配置的周期，将分钟单位转换为毫秒单位存入数组
    for (int i = 0; i < periodsStr.length; i++) {
      nntopReportingPeriodsMs[i] = Ints.checkedCast(
          TimeUnit.MINUTES.toMillis(Integer.parseInt(periodsStr[i])));
    }
    // 校验所有周期不小于1分钟，满足最小周期要求
    for (int aPeriodMs: nntopReportingPeriodsMs) {
      Preconditions.checkArgument(aPeriodMs >= TimeUnit.MINUTES.toMillis(1),
          "minimum reporting period is 1 min!");
    }
  }
}