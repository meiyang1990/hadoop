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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN ResourceManager Web REST API 中，资源预留更新请求的响应数据对象。
 * 用于封装预约更新操作的返回结果，供Web UI序列化返回给客户端。
 */
@XmlRootElement(name = "reservation-update-response")
@XmlAccessorType(XmlAccessType.FIELD)
public class ReservationUpdateResponseInfo {

  /**
   * JAXB反序列化需要的无参构造函数。
   */
  public ReservationUpdateResponseInfo() {

  }

}