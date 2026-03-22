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
 * MR应用Web服务自定义JSON序列化提供者，用于排除根元素包裹，生成符合前端要求的干净JSON格式。
 * 继承MOXyJsonProvider实现，只处理配置中指定的类，关闭根元素包装和空集合输出。
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ExcludeRootJSONProvider extends MOXyJsonProvider {

  private final static Logger LOG = LoggerFactory.getLogger(ExcludeRootJSONProvider.class);
  // 类序列化配置，保存需要排除根元素的类集合
  private final ClassSerializationConfig classSerializationConfig;

  /**
   * 构造函数，初始化排除根元素的类配置。
   */
  @Inject
  public ExcludeRootJSONProvider() {
    classSerializationConfig = new ClassSerializationConfig();
  }

  /**
   * 判断当前提供者是否支持读取指定类型的JSON数据。
   * 仅支持配置中标记为需要解除根包裹的类。
   * @param type 待处理的类类型
   * @param genericType 泛型类型
   * @param annotations 注解数组
   * @param mediaType 媒体类型
   * @return 是否支持读取
   */
  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    boolean match = classSerializationConfig.getUnWrappedClasses().contains(type);
    LOG.trace("ExcludeRootJSONProvider compatibility with {} is {}", type, match);
    return match;
  }

  /**
   * 判断当前提供者是否支持写出指定类型的JSON数据。
   * 读写判断逻辑一致，复用isReadable方法。
   * @param type 待处理的类类型
   * @param genericType 泛型类型
   * @param annotations 注解数组
   * @param mediaType 媒体类型
   * @return 是否支持写出
   */
  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
      MediaType mediaType) {
    return isReadable(type, genericType, annotations, mediaType);
  }

  /**
   * 读取JSON前的预处理，配置取消根元素包裹。
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
    LOG.trace("ExcludeRootJSONProvider preReadFrom with {}", type);
    // 关闭JSON根元素包裹，直接读取对象内容
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
  }

  /**
   * 写出JSON前的预处理，配置取消根元素包裹和空集合输出。
   * @param object 待序列化对象
   * @param type 待处理的类类型
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
    LOG.trace("ExcludeRootJSONProvider preWriteTo with {}", type);
    // 关闭空集合序列化，减少冗余输出
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    // 关闭JSON根元素包裹，直接输出对象内容
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
  }
}