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

package org.apache.hadoop.mapred;

/**
 * 包装包私有访问权限的PeriodicStatsAccumulator，提供对外公开访问入口
 * 解决原周期性统计累加器类因包访问权限限制，无法在其他包中使用的问题
 */
//Workaround for PeriodicStateAccumulator being package access
public class WrappedPeriodicStatsAccumulator {

  // 持有的实际周期性统计累加器实例
  private PeriodicStatsAccumulator real;

  /**
   * 构造包装类，持有实际的累加器实例
   * @param real 实际的包私有周期性统计累加器实例
   */
  public WrappedPeriodicStatsAccumulator(PeriodicStatsAccumulator real) {
    this.real = real;
  }
  
  /**
   * 扩展累加统计范围，代理调用实际累加器的extend方法
   * @param newProgress 新的进度值
   * @param newValue 新增的统计值
   */
  public void extend(double newProgress, int newValue) {
    real.extend(newProgress, newValue);
  }
}