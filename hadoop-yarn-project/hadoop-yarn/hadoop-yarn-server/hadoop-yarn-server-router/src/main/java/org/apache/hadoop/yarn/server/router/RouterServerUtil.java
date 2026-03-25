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

package org.apache.hadoop.yarn.server.router;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.protobuf.GeneratedMessageV3;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.StringStringMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringBytesMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.StringLocalResourceMapProto;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.EnumSet;
import java.io.IOException;

/**
 * YARN Router服务器公共工具类，提供Router服务所需的通用工具方法。
 *
 */
@Private
@Unstable
public final class RouterServerUtil {

  private static final String APPLICATION_ID_PREFIX = "application_";

  private static final String APP_ATTEMPT_ID_PREFIX = "appattempt_";

  private static final String CONTAINER_PREFIX = "container_";

  private static final String EPOCH_PREFIX = "e";

  private static final String RESERVEIDSTR_PREFIX = "reservation_";

  /** Disable constructor. */
  private RouterServerUtil() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(RouterServerUtil.class);

  /**
   * 记录错误日志并抛出YarnException异常。
   *
   * @param t 调用方抛出的异常
   * @param errMsgFormat 错误信息格式化字符串
   * @param args 格式化参数
   * @throws YarnException 抛出格式化后的异常
   */
  @Public
  @Unstable
  public static void logAndThrowException(Throwable t, String errMsgFormat, Object... args)
      throws YarnException {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      String newErrMsg = getErrorMsg(msg, t);
      LOG.error(newErrMsg, t);
      throw new YarnException(newErrMsg, t);
    } else {
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  /**
   * 记录错误日志并抛出YarnException异常。
   *
   * @param errMsg 错误信息
   * @param t 调用方抛出的异常
   * @throws YarnException 抛出格式化后的异常
   */
  @Public
  @Unstable
  public static void logAndThrowException(String errMsg, Throwable t)
      throws YarnException {
    if (t != null) {
      String newErrMsg = getErrorMsg(errMsg, t);
      LOG.error(newErrMsg, t);
      throw new YarnException(newErrMsg, t);
    } else {
      LOG.error(errMsg);
      throw new YarnException(errMsg);
    }
  }

  /**
   * 记录错误日志并抛出YarnException异常。
   *
   * @param errMsg 错误信息
   * @throws YarnException 抛出异常
   */
  @Public
  @Unstable
  public static void logAndThrowException(String errMsg) throws YarnException {
    LOG.error(errMsg);
    throw new YarnException(errMsg);
  }

  private static String getErrorMsg(String errMsg, Throwable t) {
    if (t.getMessage() != null) {
      return errMsg + "" + t.getMessage();
    }
    return errMsg;
  }

  /**
   * 创建请求拦截器责任链，根据配置初始化所有拦截器并串联成链。
   *
   * @param conf 配置对象
   * @param pipeLineClassName 责任链配置键名
   * @param interceptorClassName 默认拦截器类名
   * @param clazz 拦截器接口类型
   * @param <R> 拦截器接口泛型
   * @return 责任链首节点实例
   */
  public static <R> R createRequestInterceptorChain(Configuration conf, String pipeLineClassName,
      String interceptorClassName, Class<R> clazz) {

    // 从配置中读取所有拦截器类名
    List<String> interceptorClassNames = getInterceptorClassNames(conf,
        pipeLineClassName, interceptorClassName);

    R pipeline = null;
    R current = null;

    // 遍历所有拦截器类，逐个实例化并串联
    for (String className : interceptorClassNames) {
      try {
        Class<?> interceptorClass = conf.getClassByName(className);
        if (clazz.isAssignableFrom(interceptorClass)) {
          // 实例化拦截器
          Object interceptorInstance = ReflectionUtils.newInstance(interceptorClass, conf);
          // 第一个拦截器作为链头
          if (pipeline == null) {
            pipeline = clazz.cast(interceptorInstance);
            current = clazz.cast(interceptorInstance);
            continue;
          } else {
            // 设置当前节点的下一个拦截器，移动当前指针
            Method method = clazz.getMethod("setNextInterceptor", clazz);
            method.invoke(current, interceptorInstance);
            current = clazz.cast(interceptorInstance);
          }
        } else {
          // 类型不匹配，记录日志并抛出异常
          LOG.error("Class: {} not instance of {}.", className, clazz.getCanonicalName());
          throw new YarnRuntimeException("Class: " + className + " not instance of "
              + clazz.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        // 找不到类，记录日志并抛出异常
        LOG.error("Could not instantiate RequestInterceptor: {}", className, e);
        throw new YarnRuntimeException("Could not instantiate RequestInterceptor: " + className, e);
      } catch (InvocationTargetException e) {
        // 调用setNextInterceptor方法异常，记录日志并抛出异常
        LOG.error("RequestInterceptor {} call setNextInterceptor error.", className, e);
        throw new YarnRuntimeException("RequestInterceptor " + className
            + " call setNextInterceptor error.", e);
      } catch (NoSuchMethodException e) {
        // 找不到setNextInterceptor方法，记录日志并抛出异常
        LOG.error("RequestInterceptor {} does not contain the method setNextInterceptor.",
            className);
        throw new YarnRuntimeException("RequestInterceptor " + className +
            " does not contain the method setNextInterceptor.", e);
      } catch (IllegalAccessException e) {
        // 无权限访问setNextInterceptor方法，记录日志并抛出异常
        LOG.error("RequestInterceptor {} call the method setNextInterceptor " +
            "does not have access.", className);
        throw new YarnRuntimeException("RequestInterceptor "
            + className + " call the method setNextInterceptor does not have access.", e);
      }
    }

    // 未配置任何拦截器，抛出异常
    if (pipeline == null) {
      throw new YarnRuntimeException(
          "RequestInterceptor pipeline is not configured in the system.");
    }

    return pipeline;
  }

  /**
   * 从配置中解析拦截器类名列表。
   *
   * @param conf 配置对象
   * @param pipeLineClass 配置键名
   * @param interceptorClass 默认拦截器类名
   * @return 拦截器类名列表
   */
  private static List<String> getInterceptorClassNames(Configuration conf,
      String pipeLineClass, String interceptorClass) {
    // 读取配置，使用默认值兜底
    String configuredInterceptorClassNames = conf.get(pipeLineClass, interceptorClass);
    List<String> interceptorClassNames = new ArrayList<>();
    // 分割字符串并去除首尾空格
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }
    return interceptorClassNames;
  }

  /**
   * 记录错误日志并抛出IOException异常。
   *
   * @param errMsg 错误信息
   * @param t 调用方抛出的异常
   * @throws IOException 抛出异常
   */
  @Public
  @Unstable
  public static void logAndThrowIOException(String errMsg, Throwable t)
      throws IOException {
    if (t != null) {
      String newErrMsg = getErrorMsg(errMsg, t);
      LOG.error(newErrMsg, t);
      throw new IOException(newErrMsg, t);
    } else {
      LOG.error(errMsg);
      throw new IOException(errMsg);
    }
  }

  /**
   * 记录错误日志并抛出IOException异常。
   *
   * @param t 调用方抛出的异常
   * @param errMsgFormat 错误信息格式化字符串
   * @param args 格式化参数
   * @throws IOException 抛出格式化后的异常
   */
  @Public
  @Unstable
  public static void logAndThrowIOException(Throwable t, String errMsgFormat, Object... args)
      throws IOException {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      String newErrMsg = getErrorMsg(msg, t);
      LOG.error(newErrMsg, t);
      throw new IOException(newErrMsg, t);
    } else {
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * 记录错误日志并抛出RuntimeException异常。
   *
   * @param errMsg 错误信息
   * @param t 调用方抛出的异常
   * @throws RuntimeException 抛出异常
   */
  @Public
  @Unstable
  public static void logAndThrowRunTimeException(String errMsg, Throwable t)
      throws RuntimeException {
    if (t != null) {
      String newErrMsg = getErrorMsg(errMsg, t);
      LOG.error(newErrMsg, t);
      throw new RuntimeException(newErrMsg, t);
    } else {
      LOG.error(errMsg);
      throw new RuntimeException(errMsg);
    }
  }

  /**
   * 记录错误日志并抛出RuntimeException异常。
   *
   * @param t 调用方抛出的异常
   * @param errMsgFormat 错误信息格式化字符串
   * @param args 格式化参数
   * @throws RuntimeException 抛出格式化后的异常
   */
  @Public
  @Unstable
  public static void logAndThrowRunTimeException(Throwable t, String errMsgFormat, Object... args)
      throws RuntimeException {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      String newErrMsg = getErrorMsg(msg, t);
      LOG.error(newErrMsg, t);
      throw new RuntimeException(newErrMsg, t);
    } else {
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  /**
   * 记录错误日志并返回RuntimeException异常对象。
   *
   * @param t 调用方抛出的异常
   * @param errMsgFormat 错误信息格式化字符串
   * @param args 格式化参数
   * @return 构造完成的RuntimeException异常对象
   */
  @Public
  @Unstable
  public static RuntimeException logAndReturnRunTimeException(
      Throwable t, String errMsgFormat, Object... args) {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      String newErrMsg = getErrorMsg(msg, t);
      LOG.error(newErrMsg, t);
      return new RuntimeException(newErrMsg, t);
    } else {
      LOG.error(msg);
      return new RuntimeException(msg);
    }
  }

  /**
   * 记录错误日志并返回RuntimeException异常对象。
   *
   * @param errMsgFormat 错误信息格式化字符串
   * @param args 格式化参数
   * @return 构造完成的RuntimeException异常对象
   */
  @Public
  @Unstable
  public static RuntimeException logAndReturnRunTimeException(
      String errMsgFormat, Object... args) {
    return logAndReturnRunTimeException(null, errMsgFormat, args);
  }

  /**
   * 记录错误日志并返回YarnRuntimeException异常对象。
   *
   * @param t 调用方抛出的异常
   * @param errMsgFormat 错误信息格式化字符串
   * @param args 格式化参数
   * @return 构造完成的YarnRuntimeException异常对象
   */
  @Public
  @Unstable
  public static YarnRuntimeException logAndReturnYarnRunTimeException(
      Throwable t, String errMsgFormat, Object... args) {
    String msg = String.format(errMsgFormat, args);
    if (t != null) {
      String newErrMsg = getErrorMsg(msg, t);
      LOG.error(newErrMsg, t);
      return new YarnRuntimeException(newErrMsg, t);
    } else {
      LOG.error(msg);
      return new YarnRuntimeException(msg);
    }
  }

  /**
   * 验证字符串格式的应用ID是否合法。
   *
   * @param applicationId 字符串格式的应用ID
   * @throws IllegalArgumentException 格式不合法时抛出异常
   */
  @Public
  @Unstable
  public static void validateApplicationId(String applicationId)
      throws IllegalArgumentException {

    // 检查应用ID非空
    if (applicationId == null || applicationId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appId is empty or null.");
    }

    // 检查前缀正确
    if (!applicationId.startsWith(APPLICATION_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid ApplicationId prefix: "
          + applicationId + ". The valid ApplicationId should start with prefix application");
    }

    // 检查分隔符位置正确
    int pos1 = APPLICATION_ID_PREFIX.length() - 1;
    int pos2 = applicationId.indexOf('_', pos1 + 1);
    if (pos2 < 0) {
      throw new IllegalArgumentException("Invalid ApplicationId: " + applicationId);
    }

    // 检查RM ID和应用ID都是数字
    String rmId = applicationId.substring(pos1 + 1, pos2);
    String appId = applicationId.substring(pos2 + 1);
    if(!NumberUtils.isDigits(rmId) || !NumberUtils.isDigits(appId)){
      throw new IllegalArgumentException("Invalid ApplicationId: " + applicationId);
    }
  }

  /**
   * 验证字符串格式的应用尝试ID是否合法。
   *
   * @param appAttemptId 字符串格式的应用尝试ID
   * @throws IllegalArgumentException 格式不合法时抛出异常
   */
  @Public
  @Unstable
  public static void validateApplicationAttemptId(String appAttemptId)
      throws IllegalArgumentException {

    // 检查应用尝试ID非空
    if (appAttemptId == null || appAttemptId.isEmpty()) {
      throw new IllegalArgumentException("Parameter error, the appAttemptId is empty or null.");
    }

    // 检查前缀正确
    if (!appAttemptId.startsWith(APP_ATTEMPT_ID_PREFIX)) {