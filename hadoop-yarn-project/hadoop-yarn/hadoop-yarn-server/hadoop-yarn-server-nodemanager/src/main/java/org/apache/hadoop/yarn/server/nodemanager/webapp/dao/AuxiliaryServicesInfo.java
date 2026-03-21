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
package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collection;

/**
 * NodeManager Web UI 数据访问对象，封装已加载的辅助服务列表信息，用于REST接口返回数据。
 */
@XmlRootElement(name = "services")
@XmlAccessorType(XmlAccessType.FIELD)
public class AuxiliaryServicesInfo {
  private ArrayList<AuxiliaryServiceInfo> service = new
      ArrayList<>();

  /**
   * 默认无参构造函数，供JAXB序列化/反序列化使用。
   */
  public AuxiliaryServicesInfo() {
    // JAXB needs this
  }

  /**
   * 添加单个辅助服务信息到列表。
   * @param s 辅助服务记录
   */
  public void add(AuxServiceRecord s) {
    service.add(new AuxiliaryServiceInfo(s.getName(), s.getVersion(), s
        .getLaunchTime()));
  }

  /**
   * 批量添加多个辅助服务信息到列表。
   * @param serviceList 辅助服务记录集合
   */
  public void addAll(Collection<AuxServiceRecord> serviceList) {
    for (AuxServiceRecord s : serviceList) {
      add(s);
    }
  }

  /**
   * 获取所有辅助服务信息列表。
   * @return 辅助服务信息列表
   */
  public ArrayList<AuxiliaryServiceInfo> getServices() {
    return service;
  }
}