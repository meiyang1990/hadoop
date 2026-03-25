// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * 单个GPU显卡的温度信息数据对象，用于NodeManager Web UI展示GPU温度信息，支持从NVIDIA nvidia-smi命令输出XML解析数据
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "temperature")
public class PerGpuTemperature {
  private float currentGpuTemp = Float.MIN_VALUE;
  private float maxGpuTemp = Float.MIN_VALUE;
  private float slowThresholdGpuTemp = Float.MIN_VALUE;

  /**
   * 获取GPU当前温度，单位：摄氏度
   * @return 当前GPU温度
   */
  @XmlJavaTypeAdapter(PerGpuDeviceInformation.StrToFloatBeforeSpaceAdapter.class)
  @XmlElement(name = "gpu_temp")
  public Float getCurrentGpuTemp() {
    return currentGpuTemp;
  }

  public void setCurrentGpuTemp(Float currentGpuTemp) {
    this.currentGpuTemp = currentGpuTemp;
  }

  /**
   * 获取GPU允许的最高温度阈值，单位：摄氏度
   * @return GPU最高温度阈值
   */
  @XmlJavaTypeAdapter(PerGpuDeviceInformation.StrToFloatBeforeSpaceAdapter.class)
  @XmlElement(name = "gpu_temp_max_threshold")
  public Float getMaxGpuTemp() {
    return maxGpuTemp;
  }

  public void setMaxGpuTemp(Float maxGpuTemp) {
    this.maxGpuTemp = maxGpuTemp;
  }

  /**
   * 获取GPU降频阈值温度，超过此温度GPU会降速运行，单位：摄氏度
   * @return GPU降频温度阈值
   */
  @XmlJavaTypeAdapter(PerGpuDeviceInformation.StrToFloatBeforeSpaceAdapter.class)
  @XmlElement(name = "gpu_temp_slow_threshold")
  public Float getSlowThresholdGpuTemp() {
    return slowThresholdGpuTemp;
  }

  public void setSlowThresholdGpuTemp(Float slowThresholdGpuTemp) {
    this.slowThresholdGpuTemp = slowThresholdGpuTemp;
  }
}