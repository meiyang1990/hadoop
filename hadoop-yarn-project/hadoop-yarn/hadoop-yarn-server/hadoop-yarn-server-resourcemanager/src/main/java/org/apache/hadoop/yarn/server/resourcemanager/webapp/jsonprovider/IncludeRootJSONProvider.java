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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.jsonprovider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.rs.MOXyJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

/**
 * YARN ResourceManager REST API自定义JSON序列化提供者，继承EclipseLink MOXyJsonProvider
 * 核心功能：对指定类开启JSON根元素包裹，统一控制JSON序列化格式。
 * <p>
 * 集成JAX-RS运行时，专门处理application/json类型的请求和响应，通过ClassSerialisationConfig
 * 决定哪些类在序列化/反序列化时需要包含根元素。
 * </p>
 *
 * 序列化/反序列化时会设置以下MOXy属性：
 * <ul>
 *   <li>{@code MarshallerProperties.JSON_INCLUDE_ROOT = true} - 开启根元素包裹</li>
 *   <li>{@code MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS = false} - 不输出空集合</li>
 * </ul>
 * 保证输出JSON结构一致，同时减少冗余数据。
 *
 * <p>
 * 提供Trace级日志，方便调试实体类型的数据绑定行为。
 * </p>
 *
 * @see org.eclipse.persistence.jaxb.rs.MOXyJsonProvider
 * @see org.eclipse.persistence.jaxb.MarshallerProperties
 * @see ClassSerialisationConfig
 * @see Configuration
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IncludeRootJSONProvider extends MOXyJsonProvider {

  private final static Logger LOG = LoggerFactory.getLogger(IncludeRootJSONProvider.class);
  private final ClassSerialisationConfig classSerialisationConfig;

  /**
   * 默认构造函数，使用空配置初始化。
   */
  public IncludeRootJSONProvider() {
    this(new Configuration());
  }

  /**
   * 依赖注入构造函数，通过Hadoop配置初始化序列化配置。
   *
   * @param conf 注入的Hadoop应用配置实例，用于初始化需要根元素包裹的类列表
   */
  @Inject
  public IncludeRootJSONProvider(@javax.inject.Named("conf") Configuration conf) {
    classSerialisationConfig = new ClassSerialisationConfig(conf);
  }

  /**
   * 判断当前提供者是否支持对指定类型进行反序列化，仅处理配置中明确指定需要根元素的类。
   */
  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    boolean match = classSerialisationConfig.getWrappedClasses().contains(type);
    LOG.trace("IncludeRootJSONProvider compatibility with {} is {}", type, match);
    return match;
  }

  /**
   * 判断当前提供者是否支持对指定类型进行序列化，逻辑同反序列化判断。
   */
  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    return isReadable(type, genericType, annotations, mediaType);
  }

  /**
   * 反序列化前预处理，开启根元素解析配置。
   */
  @Override
  protected void preReadFrom(Class<Object> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, Unmarshaller unmarshaller)
      throws JAXBException {
    LOG.trace("IncludeRootJSONProvider preReadFrom with {}", type);
    // 开启根元素包含，正确解析带根节点的JSON
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);
  }

  /**
   * 序列化前预处理，配置输出属性。
   */
  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
      Marshaller marshaller) throws JAXBException {
    LOG.trace("IncludeRootJSONProvider preWriteTo with {}", type);
    // 不序列化空集合，减少输出冗余
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    // 开启根元素包含，输出带根节点的JSON结构
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);
  }
}