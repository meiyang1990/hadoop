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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;

/**
 * 带自定义类加载器的辅助服务包装类
 * 用于支持第三方辅助服务使用独立的类加载器隔离加载，避免类冲突
 */
final class AuxiliaryServiceWithCustomClassLoader extends AuxiliaryService {

  private final AuxiliaryService wrapped;
  private final ClassLoader customClassLoader;

  /**
   * 构造带自定义类加载器的包装实例
   * @param name 辅助服务名称
   * @param wrapped 被包装的原始辅助服务实例
   * @param customClassLoader 自定义类加载器
   */
  private AuxiliaryServiceWithCustomClassLoader(String name,
      AuxiliaryService wrapped, ClassLoader customClassLoader) {
    super(name);
    this.wrapped = wrapped;
    this.customClassLoader = customClassLoader;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 创建配置副本，避免自定义类加载器加载类时出现ClassNotFoundException
    Configuration config = new Configuration(conf);
    // 重置当前服务的配置
    setConfig(config);
    // 为配置设置自定义类加载器
    config.setClassLoader(customClassLoader);
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务执行初始化
      wrapped.init(config);
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务启动
      wrapped.start();
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务停止
      wrapped.stop();
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void initializeApplication(
      ApplicationInitializationContext initAppContext) {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务初始化应用
      wrapped.initializeApplication(initAppContext);
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext stopAppContext) {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务停止应用
      wrapped.stopApplication(stopAppContext);
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public ByteBuffer getMetaData() {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务获取元数据并返回
      return wrapped.getMetaData();
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void initializeContainer(ContainerInitializationContext
      initContainerContext) {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务初始化容器
      wrapped.initializeContainer(initContainerContext);
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void stopContainer(ContainerTerminationContext stopContainerContext) {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务停止容器
      wrapped.stopContainer(stopContainerContext);
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Override
  public void setRecoveryPath(Path recoveryPath) {
    // 保存原上下文类加载器
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    // 切换上下文为自定义类加载器
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      // 委托原服务设置恢复路径
      wrapped.setRecoveryPath(recoveryPath);
    } finally {
      // 恢复原上下文类加载器
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  /**
   * 工厂方法，创建带自定义类加载器的辅助服务实例
   * @param conf 配置对象
   * @param className 辅助服务实现类全限定名
   * @param appClassPath 自定义类路径
   * @param systemClasses 系统类列表（由父加载器优先加载）
   * @return 包装好的辅助服务实例
   * @throws IOException 类加载器创建异常
   * @throws ClassNotFoundException 找不到目标服务类
   */
  public static AuxiliaryServiceWithCustomClassLoader getInstance(
      Configuration conf, String className, String appClassPath, String[]
      systemClasses) throws IOException, ClassNotFoundException {
    // 创建自定义类加载器
    ClassLoader customClassLoader = createAuxServiceClassLoader(
        appClassPath, systemClasses);
    // 使用自定义类加载器加载目标服务类
    Class<?> clazz = Class.forName(className, true,
        customClassLoader);
    // 转换为AuxiliaryService子类类型
    Class<? extends AuxiliaryService> sClass = clazz.asSubclass(
        AuxiliaryService.class);
    // 通过反射创建服务实例
    AuxiliaryService wrapped = ReflectionUtils.newInstance(sClass, conf);
    // 返回包装实例
    return new AuxiliaryServiceWithCustomClassLoader(
        className + " with custom class loader", wrapped,
        customClassLoader);
  }

  /**
   * 创建辅助服务专用的自定义类加载器
   * @param appClasspath 自定义类路径
   * @param systemClasses 系统类列表
   * @return 创建好的类加载器
   * @throws IOException 创建失败异常
   */
  private static ClassLoader createAuxServiceClassLoader(
      final String appClasspath, final String[] systemClasses)
      throws IOException {
    try {
      // 特权操作创建类加载器，保证权限正确
      return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws MalformedURLException {
            // 使用ApplicationClassLoader实现隔离加载
            return new ApplicationClassLoader(appClasspath,
                AuxServices.class.getClassLoader(),
                Arrays.asList(systemClasses));
          }
        }
      );
    } catch (PrivilegedActionException e) {
      Throwable t = e.getCause();
      if (t instanceof MalformedURLException) {
        throw (MalformedURLException) t;
      }
      throw new IOException(e);
    }
  }
}