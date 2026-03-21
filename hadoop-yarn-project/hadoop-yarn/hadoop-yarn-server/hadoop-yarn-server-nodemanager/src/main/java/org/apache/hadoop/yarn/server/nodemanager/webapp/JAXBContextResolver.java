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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import javax.inject.Singleton;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;

import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AuxiliaryServiceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AuxiliaryServicesInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMDeviceResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;

/**
 * NodeManager Web UI JAXB上下文解析器，为REST JSON/XML序列化提供上下文支持
 * 注册所有需要序列化的DAO类，统一配置JSON输出格式
 */
@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private JAXBContext context;
  private final Set<Class> types;

  // 所有需要JAXB序列化的DAO类列表
  private final Class[] cTypes = {AppInfo.class, AppsInfo.class,
      AuxiliaryServicesInfo.class, AuxiliaryServiceInfo.class,
      ContainerInfo.class, ContainersInfo.class, NodeInfo.class,
      RemoteExceptionData.class, NMGpuResourceInfo.class, NMResourceInfo.class,
      NMDeviceResourceInfo.class};

  /**
   * 构造函数，初始化JAXB上下文并配置序列化参数
   * @throws Exception 初始化失败时抛出异常
   */
  public JAXBContextResolver() throws Exception {
    // 将所有需要序列化的类存入集合方便查询
    this.types = new HashSet<>(Arrays.asList(cTypes));
    // 配置JSON输出格式，使JSON输出结构与XML输出保持一致，不包含根节点包装
    this.context = JAXBContextFactory.createContext(cTypes, Collections.singletonMap(
        MarshallerProperties.JSON_INCLUDE_ROOT, false));
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    // 当前类已注册时返回共享上下文，否则返回null交由其他处理
    return (types.contains(objectType)) ? context : null;
  }
}