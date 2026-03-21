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
 * 自定义JSON序列化提供器，继承自MOXyJsonProvider，为指定类提供不包含根节点的JSON序列化/反序列化能力
 * <p>
 * 该提供器集成EclipseLink MOXy与JAX-RS运行时，支持application/json类型的读写。
 * 通过{@link ClassSerialisationConfig}配置确定哪些类需要去掉根节点进行JSON序列化/反序列化。
 * </p>
 *
 * 序列化/反序列化过程中会设置以下MOXy属性：
 * <ul>
 *   <li>{@code MarshallerProperties.JSON_INCLUDE_ROOT = false} - 不包含根节点</li>
 *   <li>{@code MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS = false} - 不序列化空集合</li>
 * </ul>
 * 该实现适合需要扁平化JSON结构或精简响应负载的API交互场景。
 *
 * <p>
 * 提供trace级日志，方便开发者排查实体类型匹配和JSON绑定问题。
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
public class ExcludeRootJSONProvider extends MOXyJsonProvider {

  private final static Logger LOG = LoggerFactory.getLogger(ExcludeRootJSONProvider.class);
  private final ClassSerialisationConfig classSerialisationConfig;

  /**
   * 默认构造器，使用空配置初始化
   */
  public ExcludeRootJSONProvider() {
    this(new Configuration());
  }

  /**
   * 带依赖注入的构造器，根据Hadoop配置初始化序列化配置
   * <p>
   * 供DI框架注入全局配置，用于创建{@link ClassSerialisationConfig}实例，
   * 该实例控制哪些类需要去掉根节点进行JSON序列化。
   * </p>
   *
   * @param conf 框架注入的应用配置对象，用于初始化序列化设置
   */
  @Inject
  public ExcludeRootJSONProvider(@javax.inject.Named("conf") Configuration conf) {
    classSerialisationConfig = new ClassSerialisationConfig(conf);
  }

  /**
   * 判断当前类型是否可由本提供器反序列化
   */
  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    // 检查当前类型是否配置为需要去掉根节点
    boolean match = classSerialisationConfig.getUnWrappedClasses().contains(type);
    LOG.trace("ExcludeRootJSONProvider compatibility with {} is {}", type, match);
    return match;
  }

  /**
   * 判断当前类型是否可由本提供器序列化
   */
  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    // 读写判断逻辑一致
    return isReadable(type, genericType, annotations, mediaType);
  }

  /**
   * 反序列化前的预处理，配置MOXy不包含根节点
   */
  @Override
  protected void preReadFrom(Class<Object> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, Unmarshaller unmarshaller)
      throws JAXBException {
    LOG.trace("ExcludeRootJSONProvider preReadFrom with {}", type);
    // 设置反序列化不包含根节点
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
  }

  /**
   * 序列化前的预处理，配置MOXy不包含根节点且不序列化空集合
   */
  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
      Marshaller marshaller) throws JAXBException {
    LOG.trace("ExcludeRootJSONProvider preWriteTo with {}", type);
    // 不序列化空集合
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    // 不包含根节点
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
  }
}