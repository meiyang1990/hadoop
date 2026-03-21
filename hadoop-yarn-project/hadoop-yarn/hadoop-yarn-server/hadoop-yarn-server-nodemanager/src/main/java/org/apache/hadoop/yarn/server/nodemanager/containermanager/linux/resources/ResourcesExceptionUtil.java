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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RESOURCE_PLUGINS_FAIL_FAST;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RESOURCE_PLUGINS_FAIL_FAST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 资源插件异常处理工具类，根据配置决定是否快速失败抛出异常。
 * 当NM_RESOURCE_PLUGINS_FAIL_FAST配置为true时抛出异常，否则忽略异常继续执行。
 */
public final class ResourcesExceptionUtil {
  private ResourcesExceptionUtil() {}

  /**
   * 根据fail-fast配置决定是否抛出传入的异常。
   * @param e 需要处理的Yarn异常
   * @param conf Yarn配置对象
   * @throws YarnException 当fail-fast开启时抛出原异常
   */
  public static void throwIfNecessary(YarnException e, Configuration conf)
      throws YarnException {
    if (conf.getBoolean(NM_RESOURCE_PLUGINS_FAIL_FAST,
        DEFAULT_NM_RESOURCE_PLUGINS_FAIL_FAST)) {
      throw e;
    }
  }
}