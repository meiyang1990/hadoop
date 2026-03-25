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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.lang.reflect.Method;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

/**
 * 应用历史服务Web端JAXB上下文工厂，复用预创建的JAXB上下文提升XML序列化性能
 * 为DAO类缓存复用JAXBContext实例，避免重复创建的性能开销
 */
public final class ContextFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContextFactory.class);

  // 缓存全局复用的JAXB上下文实例
  private static JAXBContext cacheContext;

  // 存储所有支持JAXB序列化的DAO和时间线实体类
  // 排除了TimelineEntity和TimelineEntities，因为它们存在JAXB兼容性问题
  private static final Class[] CTYPES = {AppInfo.class, AppsInfo.class,
      AppAttemptInfo.class, AppAttemptsInfo.class, ContainerInfo.class,
      ContainersInfo.class, RemoteExceptionData.class, TimelineDomain.class,
      TimelineDomains.class, TimelineEvents.class, TimelinePutResponse.class};
  private static final Set<Class> CLASS_SET =
      new HashSet<>(Arrays.asList(CTYPES));

  // 需要忽略的类型：TimelineEntity包含Set接口，JAXB无法处理会抛出注解异常
  private static final Class[] IGNORE_TYPES = {TimelineEntity.class,
      TimelineEntities.class};
  private static final Set<Class> IGNORE_SET =
      new HashSet<>(Arrays.asList(IGNORE_TYPES));

  // 预创建忽略类型对应的异常实例，用于快速抛出
  private static JAXBException je =
      new JAXBException("TimelineEntity and TimelineEntities has " +
      "IllegalAnnotation");

  // 预定义栈追踪信息，标识异常来源于本工厂类
  private static StackTraceElement[] stackTrace = new StackTraceElement[]{
      new StackTraceElement(ContextFactory.class.getName(),
      "createContext", "ContextFactory.java", -1)};

  // 工具类不允许实例化
  private ContextFactory() {
  }

  /**
   * 通过反射调用JAXB RI原生工厂创建JAXB上下文
   * @param classes 需要绑定的类数组
   * @param properties JAXB配置属性
   * @return 创建好的JAXB上下文实例
   * @throws Exception 反射调用或创建上下文失败时抛出
   */
  public static JAXBContext newContext(Class[] classes,
      Map<String, Object> properties) throws Exception {
    Class spFactory = Class.forName(
        "com.sun.xml.bind.v2.ContextFactory");
    Method m = spFactory.getMethod("createContext", Class[].class, Map.class);
    return (JAXBContext) m.invoke(null, classes, properties);
  }

  /**
   * 创建并缓存JAXB上下文，由WebComponent.service方法调用
   * 对预定义类复用缓存上下文，忽略不兼容类型，处理未缓存类的动态创建
   * @param classes 需要绑定的类数组
   * @param properties JAXB配置属性
   * @return 创建或缓存的JAXB上下文实例
   * @throws Exception 遇到不兼容类型、创建上下文失败时抛出
   */
  // Called from WebComponent.service
  public static JAXBContext createContext(Class[] classes,
      Map<String, Object> properties) throws Exception {
    // 遍历检查每个需要绑定的类
    for (Class c : classes) {
      // 如果是需要忽略的不兼容类型，直接抛出预定义异常
      if (IGNORE_SET.contains(c)) {
        je.setStackTrace(stackTrace);
        throw je;
      }
      // 如果不在预定义缓存类集合中，动态创建新上下文
      if (!CLASS_SET.contains(c)) {
        try {
          return newContext(classes, properties);
        } catch (Exception e) {
          LOG.warn("Error while Creating JAXBContext", e);
          throw e;
        }
      }
    }

    // 所有类都在预定义集合中，懒加载创建缓存上下文
    try {
      synchronized (ContextFactory.class) {
        if (cacheContext == null) {
          cacheContext = newContext(CTYPES, properties);
        }
      }
    } catch(Exception e) {
      LOG.warn("Error while Creating JAXBContext", e);
      throw e;
    }
    return cacheContext;
  }

  /**
   * 根据上下文路径创建JAXB上下文，由WebComponent.init方法调用
   * 通过反射调用JAXB RI原生工厂实现
   * @param contextPath JAXB上下文路径
   * @param classLoader 类加载器
   * @param properties JAXB配置属性
   * @return 创建好的JAXB上下文实例
   * @throws Exception 反射调用或创建上下文失败时抛出
   */
  // Called from WebComponent.init
  public static JAXBContext createContext(String contextPath, ClassLoader
      classLoader, Map<String, Object> properties) throws Exception {
    Class spFactory = Class.forName(
        "com.sun.xml.bind.v2.ContextFactory");
    Method m = spFactory.getMethod("createContext", String.class,
        ClassLoader.class, Map.class);
    return (JAXBContext) m.invoke(null, contextPath, classLoader,
        properties);
  }

}