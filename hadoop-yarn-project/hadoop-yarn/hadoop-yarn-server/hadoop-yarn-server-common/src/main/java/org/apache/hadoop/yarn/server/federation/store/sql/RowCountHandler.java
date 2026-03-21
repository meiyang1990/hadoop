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

import org.apache.hadoop.util.StringUtils;

import java.sql.SQLException;

/**
 * YARN联邦SQL存储行数结果处理器
 * 用于从存储过程输出参数中解析获取符合条件的总行数，支持分页查询场景。
 */
public class RowCountHandler implements ResultSetHandler<Integer> {

  /** 存储行数结果的输出参数名称 */
  private String rowCountParamName;

  /**
   * 构造行数结果处理器，指定目标输出参数名称。
   * @param paramName 存储行数结果的输出参数名称
   */
  public RowCountHandler(String paramName) {
    this.rowCountParamName = paramName;
  }

  @Override
  // 遍历输入参数，从输出参数中提取目标行数结果
  public Integer handle(Object... params) throws SQLException {
    Integer result = 0;
    for (Object param : params) {
      // 判断当前参数是否为SQL输出参数
      if (param instanceof FederationSQLOutParameter) {
        FederationSQLOutParameter parameter = (FederationSQLOutParameter) param;
        String paramName = parameter.getParamName();
        Object parmaValue = parameter.getValue();
        // 匹配参数名称，找到目标行数参数
        if (StringUtils.equalsIgnoreCase(paramName, rowCountParamName)) {
          result = getRowCount(parmaValue);
        }
      }
    }
    return result;
  }

  // 将参数值转换为整数行数结果
  private Integer getRowCount(Object rowCount) {
    return Integer.parseInt(String.valueOf(rowCount));
  }
}