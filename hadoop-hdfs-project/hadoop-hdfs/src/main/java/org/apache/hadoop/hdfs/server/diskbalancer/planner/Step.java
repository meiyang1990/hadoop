// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;

/**
 * 磁盘均衡计划中的单个数据移动步骤接口，定义了一次数据迁移操作需要暴露的核心属性和配置
 */
public interface Step {
  /**
   * 获取本次步骤需要移动的数据字节数
   *
   * @return 需要移动的字节数
   */
  long getBytesToMove();

  /**
   * 获取数据移动的目标磁盘卷
   *
   * @return 目标磁盘卷对象
   */
  DiskBalancerVolume getDestinationVolume();

  /**
   * 获取移动后预期的理想存储使用率
   *
   * @return 理想存储使用率数值
   */
  double getIdealStorage();

  /**
   * 获取数据移动的源磁盘卷
   *
   * @return 源磁盘卷对象
   */
  DiskBalancerVolume getSourceVolume();

  /**
   * 获取当前步骤所属卷集的ID
   *
   * @return 卷集ID字符串
   */
  String getVolumeSetID();

  /**
   * 将字节大小转换为人类可读的字符串表示
   *
   * @param size 字节大小
   * @return 格式化后的大小字符串
   */
  String getSizeString(long size);

  /**
   * 获取当前步骤允许容忍的最大磁盘错误数，超过该值步骤失败
   *
   * @return 允许的最大磁盘错误数
   */
  long getMaxDiskErrors();

  /**
   * 获取数据移动的容忍百分比，当实际存储使用率与理想值的差距小于该百分比时，认为达到平衡目标
   *
   * @return 容忍百分比
   */
  long getTolerancePercent();

  /**
   * 获取磁盘均衡允许使用的最大磁盘带宽，单位为MB/秒
   * 限制数据迁移过程中磁盘的带宽占用，避免影响集群正常业务
   *
   * @return 最大带宽，单位MB/秒
   */
  long getBandwidth();

  /**
   * 设置当前步骤的容忍百分比
   *
   * @param tolerancePercent 容忍百分比
   */
  void setTolerancePercent(long tolerancePercent);

  /**
   * 设置当前步骤允许使用的最大磁盘带宽
   *
   * @param bandwidth 最大带宽，单位MB/秒
   */
  void setBandwidth(long bandwidth);

  /**
   * 设置当前步骤允许容忍的最大磁盘错误数
   *
   * @param maxDiskErrors 允许的最大磁盘错误数
   */
  void setMaxDiskErrors(long maxDiskErrors);

}