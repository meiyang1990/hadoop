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

package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;


@Private
@InterfaceStability.Unstable
// 应用级 Timeline collector 的元数据，记录地址、RM 时间戳与版本以便比较新旧
public abstract class AppCollectorData {

  protected static final long DEFAULT_TIMESTAMP_VALUE = -1;

  // 工厂方法，创建包含完整 RM 时间戳、版本和访问 token 的 collector 记录
  public static AppCollectorData newInstance(
      ApplicationId id, String collectorAddr, long rmIdentifier, long version,
      Token token) {
    // 通过 PB record 创建实例并填充 collector 地址、RM 时间戳和版本
    AppCollectorData appCollectorData =
        Records.newRecord(AppCollectorData.class);
    appCollectorData.setApplicationId(id);
    appCollectorData.setCollectorAddr(collectorAddr);
    appCollectorData.setRMIdentifier(rmIdentifier);
    appCollectorData.setVersion(version);
    appCollectorData.setCollectorToken(token);
    return appCollectorData;
  }

  public static AppCollectorData newInstance(
      ApplicationId id, String collectorAddr, long rmIdentifier, long version) {
    // 便捷重载，缺省 collector token 但仍携带 RM 时间戳与版本
    return newInstance(id, collectorAddr, rmIdentifier, version, null);
  }

  public static AppCollectorData newInstance(ApplicationId id,
      String collectorAddr, Token token) {
    // 用默认时间戳占位，表示还未被 RM 认可，仅提供 collector 地址和 token
    return newInstance(id, collectorAddr, DEFAULT_TIMESTAMP_VALUE,
        DEFAULT_TIMESTAMP_VALUE, token);
  }

  public static AppCollectorData newInstance(ApplicationId id,
      String collectorAddr) {
    return newInstance(id, collectorAddr, null);
  }

  /**
   * Returns if a collector data item happens before another one. Null data
   * items happens before any other non-null items. Non-null data items A
   * happens before another non-null item B when A's rmIdentifier is less than
   * B's rmIdentifier. Or A's version is less than B's if they have the same
   * rmIdentifier.
   *
   * @param dataA first collector data item.
   * @param dataB second collector data item.
   * @return true if dataA happens before dataB.
   */
  // 比较两条 collector 记录的先后顺序：null 视为最旧，先比 RM 时间戳再比版本号
  public static boolean happensBefore(AppCollectorData dataA,
      AppCollectorData dataB) {
    if (dataA == null && dataB == null) {
      return false;
    } else if (dataA == null || dataB == null) {
      return dataA == null;
    }

    return
        (dataA.getRMIdentifier() < dataB.getRMIdentifier())
        || ((dataA.getRMIdentifier() == dataB.getRMIdentifier())
            && (dataA.getVersion() < dataB.getVersion()));
  }

  /**
   * Returns if the collector data has been stamped by the RM with a RM cluster
   * timestamp and a version number.
   *
   * @return true if RM has already assigned a timestamp for this collector.
   * Otherwise, it means the RM has not recognized the existence of this
   * collector.
   */
  // 判断是否已被 RM 赋予时间戳和版本号，未赋值意味着 RM 尚未确认该 collector
  public boolean isStamped() {
    return (getRMIdentifier() != DEFAULT_TIMESTAMP_VALUE)
        || (getVersion() != DEFAULT_TIMESTAMP_VALUE);
  }

  public abstract ApplicationId getApplicationId();

  public abstract void setApplicationId(ApplicationId id);

  public abstract String getCollectorAddr();

  public abstract void setCollectorAddr(String addr);

  public abstract long getRMIdentifier();

  public abstract void setRMIdentifier(long rmId);

  public abstract long getVersion();

  public abstract void setVersion(long version);

  /**
   * Get delegation token for app collector which AM will use to publish
   * entities.
   * @return the delegation token for app collector.
   */
  public abstract Token getCollectorToken();

  public abstract void setCollectorToken(Token token);
}
