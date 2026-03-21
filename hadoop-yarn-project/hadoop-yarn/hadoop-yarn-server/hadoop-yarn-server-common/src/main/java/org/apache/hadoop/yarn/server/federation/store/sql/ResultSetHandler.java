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

import java.sql.SQLException;

/**
 * YARN联邦元数据SQL查询结果处理器接口，定义将JDBC查询结果转换为目标对象的统一规范。
 *
 * @param <T> 转换后目标对象的类型
 */
public interface ResultSetHandler<T> {
  /**
   * 处理SQL查询结果，将其转换为指定类型的业务对象。
   * @param params 查询参数与结果集对象
   * @return 转换完成的业务对象
   * @throws SQLException 处理结果集过程中发生SQL异常时抛出
   */
  T handle(Object... params) throws SQLException;
}