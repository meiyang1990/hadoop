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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider.ClassSerialisationConfig;

/**
 * JAXB 上下文解析器，为YARN ResourceManager REST API提供不同类型的JAXB上下文，
 * 支持带根元素和不带根元素两种JSON序列化配置，区分不同类型的序列化需求。
 */
@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {
  private static final Logger LOG = LoggerFactory.getLogger(JAXBContextResolver.class.getName());
  // 类型到对应JAXB上下文的映射缓存
  private final Map<Class, JAXBContext> typesContextMap = new HashMap<>();

  /**
   * 默认构造函数，使用空配置创建解析器。
   * @throws Exception 创建上下文失败时抛出异常
   */
  public JAXBContextResolver() throws Exception {
    this(new Configuration());
  }

  /**
   * 依赖注入构造函数，使用传入配置初始化序列化配置和JAXB上下文。
   * @param conf Hadoop配置对象
   * @throws Exception 创建上下文失败时抛出异常
   */
  @Inject
  public JAXBContextResolver(@javax.inject.Named("conf") Configuration conf) throws Exception {
    // 创建类序列化配置对象，从配置中加载需要特殊序列化的类
    ClassSerialisationConfig classSerialisationConfig = new ClassSerialisationConfig(conf);
    // 获取需要包裹根元素的类集合
    Set<Class<?>> wrappedClasses = classSerialisationConfig.getWrappedClasses();
    // 获取不需要包裹根元素的类集合
    Set<Class<?>> unWrappedClasses = classSerialisationConfig.getUnWrappedClasses();

    //WARNING: AFAIK these properties not respected by MOXyJsonProvider
    //For details check MOXyJsonProvider#readFrom method
    // 为需要根元素包裹的类创建JAXB上下文，启用JSON根元素包含
    JAXBContext wrappedContext = JAXBContextFactory.createContext(
        wrappedClasses.toArray(new Class[0]),
        Collections.singletonMap(MarshallerProperties.JSON_INCLUDE_ROOT, true)
    );
    // 为不需要根元素包裹的类创建JAXB上下文，禁用JSON根元素包含
    JAXBContext unWrappedContext = JAXBContextFactory.createContext(
        unWrappedClasses.toArray(new Class[0]),
        Collections.singletonMap(MarshallerProperties.JSON_INCLUDE_ROOT, false)
    );

    // 将所有需要根元素的类注册到对应上下文缓存
    wrappedClasses.forEach(type -> typesContextMap.put(type, wrappedContext));
    // 将所有不需要根元素的类注册到对应上下文缓存
    unWrappedClasses.forEach(type -> typesContextMap.put(type, unWrappedContext));
  }

  /**
   * 根据类型获取对应的JAXB上下文实例。
   * @param objectType 需要序列化/反序列化的对象类型
   * @return 对应配置的JAXB上下文，不存在则返回null
   */
  @Override
  public JAXBContext getContext(Class<?> objectType) {
    JAXBContext jaxbContext = typesContextMap.get(objectType);
    LOG.trace("Context for {} is {}", objectType,  jaxbContext);
    return jaxbContext;
  }
}