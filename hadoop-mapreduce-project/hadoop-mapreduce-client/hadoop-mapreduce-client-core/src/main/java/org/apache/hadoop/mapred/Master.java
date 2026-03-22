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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;

/**
 * MapReduce 主节点工具类，用于获取主节点地址和服务principal等信息
 * 兼容经典MapReduce框架和YARN框架两种运行模式
 */
@Private
@Unstable
public class Master {
  /**
   * 主节点状态枚举
   */
  public enum State {
    /** 正在初始化 */
    INITIALIZING, 
    /** 运行中 */
    RUNNING;
  }

  /**
   * 从配置中解析获取MapReduce主节点地址
   * @param conf 配置对象
   * @return 主节点主机名
   */
  public static String getMasterAddress(Configuration conf) {
    String masterAddress = conf.get(MRConfig.MASTER_ADDRESS, "localhost:8012");

    return NetUtils.createSocketAddr(masterAddress, 8012,
            MRConfig.MASTER_ADDRESS).getHostName();
  }

  /**
   * 获取MapReduce主节点的Kerberos安全主体名称，用于安全认证和代理令牌续签
   * @param conf 配置对象
   * @return 主节点Kerberos主体名称
   * @throws IOException 获取主体失败时抛出IO异常
   */
  public static String getMasterPrincipal(Configuration conf)
      throws IOException {
    String masterPrincipal;
    // 获取当前配置的运行框架类型
    String framework = conf.get(MRConfig.FRAMEWORK_NAME,
            MRConfig.YARN_FRAMEWORK_NAME);

    if (framework.equals(MRConfig.CLASSIC_FRAMEWORK_NAME)) {
      // 经典框架场景：获取本地配置的MapReduce主节点地址
      String masterAddress = getMasterAddress(conf);
      // 生成并返回主节点Kerberos服务主体
      masterPrincipal =
          SecurityUtil.getServerPrincipal(conf.get(MRConfig.MASTER_USER_NAME),
          masterAddress);
    } else {
      // YARN框架场景：从YARN获取ResourceManager服务主体
      masterPrincipal = YarnClientUtils.getRmPrincipal(conf);
    }

    return masterPrincipal;
  }
}