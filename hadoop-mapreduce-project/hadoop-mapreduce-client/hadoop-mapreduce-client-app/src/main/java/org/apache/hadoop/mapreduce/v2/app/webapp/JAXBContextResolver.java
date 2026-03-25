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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;

import javax.inject.Singleton;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.mapreduce.v2.app.webapp.jsonprovider.ClassSerializationConfig;

/**
 * MapReduce应用Web服务的JAXB上下文解析器
 * 负责根据序列化配置，为不同类型提供预初始化的JAXB上下文，支持带/不带根节点的JSON序列化
 */
@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  // 存储类型到对应JAXB上下文的映射缓存
  private final Map<Class, JAXBContext> typesContextMap = new HashMap<>();

  /**
   * 构造函数，根据序列化配置预初始化两类JAXB上下文
   * @throws Exception 初始化JAXB上下文失败时抛出异常
   */
  public JAXBContextResolver() throws Exception {
    // 加载序列化配置类
    ClassSerializationConfig classSerializationConfig = new ClassSerializationConfig();
    // 获取需要保留根节点的类型集合
    Set<Class<?>> wrappedClasses = classSerializationConfig.getWrappedClasses();
    // 获取不需要保留根节点的类型集合
    Set<Class<?>> unWrappedClasses = classSerializationConfig.getUnWrappedClasses();

    // 创建带根节点的JAXB上下文，开启JSON根节点包含选项
    JAXBContext wrappedContext = JAXBContextFactory.createContext(
        wrappedClasses.toArray(new Class[0]),
        Collections.singletonMap(MarshallerProperties.JSON_INCLUDE_ROOT, true)
    );
    // 创建不带根节点的JAXB上下文，关闭JSON根节点包含选项
    JAXBContext unWrappedContext = JAXBContextFactory.createContext(
        unWrappedClasses.toArray(new Class[0]),
        Collections.singletonMap(MarshallerProperties.JSON_INCLUDE_ROOT, false)
    );

    // 将所有带根节点的类型映射到对应上下文
    wrappedClasses.forEach(type -> typesContextMap.put(type, wrappedContext));
    // 将所有不带根节点的类型映射到对应上下文
    unWrappedClasses.forEach(type -> typesContextMap.put(type, unWrappedContext));
  }

  /**
   * 根据对象类型获取对应的预初始化JAXB上下文
   * @param objectType 待序列化/反序列化的对象类型
   * @return 对应配置的JAXB上下文，不存在则返回null
   */
  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return typesContextMap.get(objectType);
  }
}