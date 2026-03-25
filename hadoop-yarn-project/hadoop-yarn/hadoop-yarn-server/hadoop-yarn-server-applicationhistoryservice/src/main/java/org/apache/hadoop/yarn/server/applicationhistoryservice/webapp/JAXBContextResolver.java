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

/**
 * 应用历史服务Web层JAXB上下文解析器，为REST API提供XML/JSON序列化的JAXB上下文支持
 */
package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;

import org.glassfish.jersey.jettison.JettisonJaxbContext;

@Singleton
@Provider
@SuppressWarnings("rawtypes")
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private JAXBContext context;
  private final Set<Class> types;

  // 需要序列化的所有DAO类列表
  private final Class[] cTypes = { AppInfo.class, AppsInfo.class,
      AppAttemptInfo.class, AppAttemptsInfo.class, ContainerInfo.class,
      ContainersInfo.class };

  /**
   * 构造JAXB上下文解析器，初始化JAXB上下文和支持的类型集合
   * @throws Exception 初始化失败时抛出异常
   */
  public JAXBContextResolver() throws Exception {
    this.types = new HashSet<>(Arrays.asList(cTypes));
    this.context = new JettisonJaxbContext(cTypes);
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    // 如果类型在支持列表中返回预初始化的上下文，否则返回null
    return (types.contains(objectType)) ? context : null;
  }
}