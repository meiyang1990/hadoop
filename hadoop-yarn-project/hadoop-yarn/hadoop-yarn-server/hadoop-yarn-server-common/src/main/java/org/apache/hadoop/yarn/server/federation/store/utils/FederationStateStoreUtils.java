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

package org.apache.hadoop.yarn.server.federation.store.utils;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreRetriableException;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

/**
 * YARN联邦状态存储工具类，提供联邦状态存储实现通用工具方法。
 */
public final class FederationStateStoreUtils {

  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreUtils.class);

  /** 联邦状态存储连接URL配置键 */
  public final static String FEDERATION_STORE_URL = "url";

  private FederationStateStoreUtils() {
  }

  /**
   * 关闭JDBC资源，将连接归还到Hikari连接池。
   *
   * @param log 日志记录器
   * @param cstmt 存储过程语句对象
   * @param conn JDBC连接
   * @param rs 结果集对象
   * @throws YarnException 关闭失败时抛出异常
   */
  public static void returnToPool(Logger log, CallableStatement cstmt,
      Connection conn, ResultSet rs) throws YarnException {
    if (cstmt != null) {
      try {
        cstmt.close();
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close Statement",
            e);
      }
    }

    if (conn != null) {
      try {
        conn.close();
        FederationStateStoreClientMetrics.decrConnections();
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close Connection",
            e);
      }
    }

    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        logAndThrowException(log, "Exception while trying to close ResultSet",
            e);
      }
    }
  }

  /**
   * 关闭JDBC语句和连接，归还连接到连接池（无结果集版本）。
   *
   * @param log 日志记录器
   * @param cstmt 存储过程语句对象
   * @param conn JDBC连接
   * @throws YarnException 关闭失败时抛出异常
   */
  public static void returnToPool(Logger log, CallableStatement cstmt,
      Connection conn) throws YarnException {
    returnToPool(log, cstmt, conn, null);
  }

  /**
   * 关闭JDBC语句，归还连接到连接池（仅语句版本）。
   *
   * @param log 日志记录器
   * @param cstmt 存储过程语句对象
   * @throws YarnException 关闭失败时抛出异常
   */
  public static void returnToPool(Logger log, CallableStatement cstmt)
      throws YarnException {
    returnToPool(log, cstmt, null);
  }

  /**
   * 记录错误日志并抛出通用YarnException。
   *
   * @param log 日志记录器
   * @param errMsg 错误消息
   * @param t 原始异常
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowException(Logger log, String errMsg,
      Throwable t) throws YarnException {
    if (t != null) {
      log.error(errMsg, t);
      throw new YarnException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new YarnException(errMsg);
    }
  }

  /**
   * 记录错误日志并抛出FederationStateStoreException。
   *
   * @param log 日志记录器
   * @param errMsg 错误消息
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowStoreException(Logger log, String errMsg)
      throws YarnException {
    log.error(errMsg);
    throw new FederationStateStoreException(errMsg);
  }

  /**
   * 记录格式化错误日志并抛出FederationStateStoreException。
   *
   * @param log 日志记录器
   * @param errMsgFormat 错误消息格式
   * @param args 格式参数
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowStoreException(Logger log, String errMsgFormat, Object... args)
      throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    log.error(errMsg);
    throw new FederationStateStoreException(errMsg);
  }


  /**
   * 记录格式化错误日志（含原始异常）并抛出FederationStateStoreException。
   *
   * @param t 原始异常
   * @param log 日志记录器
   * @param errMsgFormat 错误消息格式
   * @param args 格式参数
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowStoreException(
      Throwable t, Logger log, String errMsgFormat, Object... args) throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    if (t != null) {
      log.error(errMsg, t);
      throw new FederationStateStoreException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new FederationStateStoreException(errMsg);
    }
  }

  /**
   * 记录错误日志并抛出FederationStateStoreInvalidInputException（输入非法异常）。
   *
   * @param log 日志记录器
   * @param errMsg 错误消息
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowInvalidInputException(Logger log, String errMsg)
      throws YarnException {
    log.error(errMsg);
    throw new FederationStateStoreInvalidInputException(errMsg);
  }

  /**
   * 记录错误日志并抛出FederationStateStoreRetriableException（可重试异常）。
   *
   * @param log 日志记录器
   * @param errMsg 错误消息
   * @param t 原始异常
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowRetriableException(Logger log, String errMsg,
      Throwable t) throws YarnException {
    if (t != null) {
      log.error(errMsg, t);
      throw new FederationStateStoreRetriableException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new FederationStateStoreRetriableException(errMsg);
    }
  }

  /**
   * 记录格式化错误日志（含原始异常）并抛出FederationStateStoreRetriableException。
   *
   * @param t 原始异常
   * @param log 日志记录器
   * @param errMsgFormat 错误消息格式
   * @param args 格式参数
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowRetriableException(
      Throwable t, Logger log, String errMsgFormat, Object... args) throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    if (t != null) {
      log.error(errMsg, t);
      throw new FederationStateStoreRetriableException(errMsg, t);
    } else {
      log.error(errMsg);
      throw new FederationStateStoreRetriableException(errMsg);
    }
  }

  /**
   * 记录格式化错误日志并抛出FederationStateStoreRetriableException。
   *
   * @param log 日志记录器
   * @param errMsgFormat 错误消息格式
   * @param args 格式参数
   * @throws YarnException 包装后的异常
   */
  public static void logAndThrowRetriableException(
      Logger log, String errMsgFormat, Object... args) throws YarnException {
    String errMsg = String.format(errMsgFormat, args);
    log.error(errMsg);
    throw new FederationStateStoreRetriableException(errMsg);
  }

  /**
   * 为Hikari数据源添加自定义连接属性。
   *
   * @param dataSource Hikari数据源对象
   * @param property 属性名称
   * @param value 属性值
   */
  public static void setProperty(HikariDataSource dataSource, String property,
      String value) {
    LOG.debug("Setting property {} with value {}", property, value);
    if (property != null && !property.isEmpty() && value != null) {
      dataSource.addDataSourceProperty(property, value);
    }
  }

  /**
   * 设置Hikari数据源连接用户名。
   *
   * @param dataSource Hikari数据源对象
   * @param userNameDB 数据库用户名
   */
  public static void setUsername(HikariDataSource dataSource,
      String userNameDB) {
    if (userNameDB != null) {
      dataSource.setUsername(userNameDB);
      LOG.debug("Setting non NULL Username for Store connection");
    } else {
      LOG.debug("NULL Username specified for Store connection, so ignoring");
    }
  }

  /**
   * 设置Hikari数据源连接密码。
   *
   * @param dataSource Hikari数据源对象
   * @param password 数据库密码
   */
  public static void setPassword(HikariDataSource dataSource, String password) {
    if (password != null) {
      dataSource.setPassword(password);
      LOG.debug("Setting non NULL Credentials for Store connection");
    } else {
      LOG.debug("NULL Credentials specified for Store connection, so ignoring");
    }
  }

  /**
   * 根据过滤条件判断是否保留该子集群的Home信息。
   *
   * @param filterSubCluster 过滤条件子集群ID，null表示不过滤
   * @param homeSubCluster 当前待判断子集群ID
   * @return 符合过滤条件返回true，否则返回false
   */
  public static boolean filterHomeSubCluster(SubClusterId filterSubCluster,
      SubClusterId homeSubCluster) {

    // 如果过滤条件为空，保留所有子集群
    if (filterSubCluster == null) {
      return true;
    }

    // 如果ID匹配则保留
    if (filterSubCluster.equals(homeSubCluster)) {
      return true;
    }

    return false;
  }

  /**
   * 将Writable对象序列化为Base64编码字符串。
   *
   * @param key 待序列化的Writable对象
   * @return Base64编码字符串
   * @throws IOException 序列化IO异常
   */
  public static String encodeWritable(Writable key) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    key.write(dos);
    dos.flush();
    return Base64.getUrlEncoder().encodeToString(bos.toByteArray());
  }

  /**
   * 将Base64编码字符串反序列化为Writable对象。
   *
   * @param w 目标Writable对象，结果将写入此对象
   * @param idStr Base64编码字符串
   * @throws IOException 反序列化IO异常
   */
  public static void decodeWritable(Writable w, String idStr) throws IOException {
    DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(Base64.getUrlDecoder().decode(idStr)));
    w.readFields(in);
  }

  /**
   * 从RouterMasterKeyRequest转换得到DelegationKey（令牌密钥）。
   * 调用前需使用FederationRouterRMTokenInputValidator验证请求非空。
   *
   * @param request 路由器主密钥请求
   * @return 转换后的DelegationKey
   */
  public static DelegationKey convertMasterKeyToDelegationKey(RouterMasterKeyRequest request) {
    RouterMasterKey masterKey = request.getRouterMasterKey();
    return convertMasterKeyToDelegationKey(masterKey);
  }

  /**
   * 从RouterMasterKey转换得到DelegationKey。
   *
   * @param masterKey 路由器存储的主密钥
   * @return 转换后的DelegationKey
   */
  private static DelegationKey convertMasterKeyToDelegationKey(RouterMasterKey masterKey) {
    ByteBuffer keyByteBuf = masterKey.getKeyBytes();
    byte[] keyBytes = new byte[keyByteBuf.remaining()];
    keyByteBuf.get(keyBytes);
    return new DelegationKey(masterKey.getKeyId(), masterKey.getExpiryDate(), keyBytes);
  }
}