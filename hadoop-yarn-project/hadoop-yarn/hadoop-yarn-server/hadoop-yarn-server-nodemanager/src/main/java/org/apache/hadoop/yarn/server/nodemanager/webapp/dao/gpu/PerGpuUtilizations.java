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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * 单块GPU的利用率信息，供NodeManager WebUI展示GPU资源使用数据
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "utilization")
public class PerGpuUtilizations {
  // GPU整体利用率百分比
  private float overallGpuUtilization;

  /**
   * 获取GPU整体利用率
   * @return 利用率百分比(0-100)
   */
  @XmlJavaTypeAdapter(PerGpuDeviceInformation.StrToFloatBeforeSpaceAdapter.class)
  @XmlElement(name = "gpu_util")
  public Float getOverallGpuUtilization() {
    return overallGpuUtilization;
  }

  public void setOverallGpuUtilization(Float overallGpuUtilization) {
    this.overallGpuUtilization = overallGpuUtilization;
  }
}