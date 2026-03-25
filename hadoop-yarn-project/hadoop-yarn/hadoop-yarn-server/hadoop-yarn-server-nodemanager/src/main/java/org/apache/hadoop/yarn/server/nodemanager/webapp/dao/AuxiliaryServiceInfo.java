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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * NodeManager Web界面DA对象，存储已加载的辅助服务信息，供Web接口序列化返回使用。
 */
@XmlRootElement(name = "service")
@XmlAccessorType(XmlAccessType.FIELD)
public class AuxiliaryServiceInfo {
  private String name;
  private String version;
  private String startTime;

  /**
   * JAXB反序列化需要的默认无参构造方法。
   */
  public AuxiliaryServiceInfo() {
    // JAXB needs this
  }

  /**
   * 构造辅助服务信息对象，格式化启动时间为指定格式字符串。
   * @param name 辅助服务名称
   * @param version 辅助服务版本
   * @param startTime 辅助服务启动时间
   */
  public AuxiliaryServiceInfo(String name, String version, Date startTime) {
    DateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    this.name = name;
    this.version = version;
    this.startTime = dateFormat.format(startTime);
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getStartTime() {
    return startTime;
  }
}