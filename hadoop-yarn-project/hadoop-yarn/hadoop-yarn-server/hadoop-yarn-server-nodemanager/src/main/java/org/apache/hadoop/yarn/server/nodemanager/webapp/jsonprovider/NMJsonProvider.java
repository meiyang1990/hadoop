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

package org.apache.hadoop.yarn.server.nodemanager.webapp.jsonprovider;

import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMDeviceResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfoes;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.rs.MOXyJsonProvider;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * NodeManager Web服务的MOXy JSON序列化提供者
 *
 * <p>该类为NodeManager REST API端点配置定制化的MOXy JSON序列化能力，支持两类JSON输出格式:</p>
 * <ul>
 *   <li>
 *     <b>带根包装</b> – JSON输出包含根包装元素，默认格式
 *   </li>
 *   <li>
 *     <b>不带根包装</b> – JSON输出省略根包装元素，用于兼容旧格式
 *   </li>
 * </ul>
 *
 * <p>该行为通过MarshallerProperties.JSON_INCLUDE_ROOT属性配置。
 * 默认NodeManager REST API响应包含根包装元素，仅少数特殊类为了兼容Jersey 1响应格式省略根包装。</p>
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NMJsonProvider extends MOXyJsonProvider {

  /**
   * 判断指定类型是否需要保留JSON根包装元素
   * @param type 待判断的类型
   * @return 是否需要根包装
   */
  private boolean isRootElementNeeded(Class<?> type) {
    return !type.equals(ContainerLogsInfoes.class)
        && !type.equals(NMGpuResourceInfo.class)
        && !type.equals(NMDeviceResourceInfo.class);
  }

  @Override
  protected void preReadFrom(Class<Object> type, Type genericType,
                             Annotation[] annotations, MediaType mediaType,
                             MultivaluedMap<String, String> httpHeaders,
                             Unmarshaller unmarshaller) throws JAXBException {
    // 反序列化前根据类型配置是否包含根元素
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, isRootElementNeeded(type));
  }

  @Override
  protected void preWriteTo(Object object, Class<?> type, Type genericType,
                            Annotation[] annotations, MediaType mediaType,
                            MultivaluedMap<String, Object> httpHeaders, Marshaller marshaller)
      throws JAXBException {
    // 序列化时不输出空集合
    marshaller.setProperty(MarshallerProperties.JSON_MARSHAL_EMPTY_COLLECTIONS, false);
    // 根据类型配置是否包含根元素
    marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, isRootElementNeeded(type));
    // 仅对ContainerLogsInfoes启用数组压缩（去掉包装，直接输出数组）
    marshaller.setProperty(
            MarshallerProperties.JSON_REDUCE_ANY_ARRAYS, type.equals(ContainerLogsInfoes.class)
    );
  }
}