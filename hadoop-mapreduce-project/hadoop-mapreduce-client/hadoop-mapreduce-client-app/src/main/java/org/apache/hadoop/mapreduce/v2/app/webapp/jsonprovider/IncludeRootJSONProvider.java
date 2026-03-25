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

package org.apache.hadoop.mapreduce.v2.app.webapp.jsonprovider;

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

/**
 * MapReduce应用Web界面的自定义JSON序列化提供者，基于EclipseLink MOXy实现。
 * 核心职责是为需要包装根节点的Java对象提供JSON序列化/反序列化能力，
 * 强制开启JSON根元素包含，适配Web API对JSON格式的要求。
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IncludeRootJSONProvider extends MOXyJsonProvider {
  // 日志记录器
  private final static Logger LOG = LoggerFactory.getLogger(IncludeRootJSONProvider.class);
  // 类序列化配置，保存需要启用根节点包装的类列表
  private final ClassSerializationConfig classSerializationConfig;

  /**
   * 构造方法，初始化类序列化配置。
   */
  @Inject
  public IncludeRootJSONProvider() {
    classSerializationConfig = new ClassSerializationConfig();
  }

  /**
   * 判断当前提供者是否支持反序列化指定类型的JSON数据。
   * @param type 待处理的类类型
   * @param genericType 泛型类型
   * @param annotations 注解数组
   * @param mediaType 媒体类型
   * @return 是否支持该类型的反序列化
   */
  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    boolean match = classSerializationConfig.getWrappedClasses().contains(type);
    LOG.trace("IncludeRootJSONProvider compatibility with {} is {}", type, match);
    return match;
  }

  /**
   * 判断当前提供者是否支持序列化指定类型为JSON数据。
   * @param type 待处理的类类型
   * @param genericType 泛型类型
   * @param annotations 注解数组
   * @param mediaType 媒体类型
   * @return 是否支持该类型的序列化
   */
  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    return isReadable(type, genericType, annotations, mediaType);
  }

  /**
   * 反序列化前的预处理，配置JSON根元素包含属性。
   * @param type 待处理的类类型
   * @param genericType 泛型类型
   * @param annotations 注解数组
   * @param mediaType 媒体类型
   * @param httpHeaders HTTP请求头
   * @param unmarshaller JAXB反序列化器
   * @throws JAXBException JAXB处理异常
   */
  @Override
  protected void preReadFrom(Class<Object> type, Type genericType, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, Unmarshaller unmarshaller)
      throws JAXBException {
    LOG.trace("IncludeRootJSONProvider preReadFrom with {}", type);
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);
  }

  /**
   * 序列化前的预处理，配置空集合不序列化和JSON根元素包含属性。
   * @param object 待序列化对象
   * @param type 对象类型
   * @param genericType 泛型类型
   * @param annotations 注解数组
   * @param mediaType 媒体类型
   * @param httpHeaders HTTP响应头
   * @param marshaller JAXB序列化器
   * @throws JAXBException JAXB处理异常
   */
  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
      Marshaller marshaller) throws JAXBException {
    LOG.trace("IncludeRootJSONProvider preWriteTo with {}", type);
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);
  }
}