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
package org.apache.hadoop.yarn.server.federation.store.sql;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * YARN联邦元数据存储SQL存储过程输出参数封装类，用于封装存储过程调用的输出参数信息
 * 配合SQL存储过程完成联邦元数据查询操作
 * @param <T> 参数泛型类型
 */
public class FederationSQLOutParameter<T> {
  private final int sqlType;
  private final Class<T> javaType;
  private T value = null;
  private String paramName;

  /**
   * 构造带参数名的输出参数
   * @param paramName 参数名称
   * @param sqlType JDBC SQL类型代码
   * @param javaType 对应Java类型
   */
  public FederationSQLOutParameter(String paramName, int sqlType, Class<T> javaType) {
    this.paramName = paramName;
    this.sqlType = sqlType;
    this.javaType = javaType;
  }

  /**
   * 构造带初始值的输出参数
   * @param sqlType JDBC SQL类型代码
   * @param javaType 对应Java类型
   * @param value 参数初始值
   */
  public FederationSQLOutParameter(int sqlType, Class<T> javaType, T value) {
    this.sqlType = sqlType;
    this.javaType = javaType;
    this.value = value;
  }

  /**
   * 获取参数对应的JDBC SQL类型代码
   * @return JDBC SQL类型代码
   */
  public int getSqlType() {
    return sqlType;
  }

  /**
   * 获取参数对应的Java类型
   * @return Java类型Class对象
   */
  public Class<T> getJavaType() {
    return javaType;
  }

  /**
   * 获取参数当前值
   * @return 参数值
   */
  public T getValue() {
    return value;
  }

  /**
   * 设置参数值
   * @param value 参数值
   */
  public void setValue(T value) {
    this.value = value;
  }

  /**
   * 获取参数名称
   * @return 参数名称
   */
  public String getParamName() {
    return paramName;
  }

  /**
   * 设置参数名称
   * @param paramName 参数名称
   */
  public void setParamName(String paramName) {
    this.paramName = paramName;
  }

  /**
   * 从CallableStatement获取输出参数值并转换为对应Java类型
   * @param stmt 可调用语句对象
   * @param index 参数索引位置
   * @throws SQLException SQL执行异常
   */
  void setValue(CallableStatement stmt, int index) throws SQLException {
    Object object = stmt.getObject(index);
    value = javaType.cast(object);
  }

  /**
   * 在CallableStatement中注册当前输出参数，若有初始值则设置
   * @param stmt 可调用语句对象
   * @param index 参数索引位置
   * @throws SQLException SQL执行异常
   */
  void register(CallableStatement stmt, int index) throws SQLException {
    stmt.registerOutParameter(index, sqlType);
    if (value != null) {
      stmt.setObject(index, value);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("OutParameter: [")
        .append("SqlType: ").append(sqlType).append(", ")
        .append("JavaType: ").append(javaType).append(", ")
        .append("Value: ").append(value)
        .append("]");
    return sb.toString();
  }
}