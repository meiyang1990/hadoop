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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AM代理IP过滤器初始化器，负责在Web服务启动时初始化AmIpFilter，
 * 配置代理主机地址、代理URI基础路径以及RM高可用场景下的RM地址信息
 */
public class AmFilterInitializer extends FilterInitializer {
  // 过滤器注册名称
  private static final String FILTER_NAME = "AM_PROXY_FILTER";
  // 过滤器实现类全限定名
  private static final String FILTER_CLASS = AmIpFilter.class.getCanonicalName();
  // RM HA URL集合参数名
  public static final String RM_HA_URLS = "RM_HA_URLS";
  
  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    // 存储过滤器初始化参数
    Map<String, String> params = new HashMap<>();
    // 从配置中获取AM过滤所需的代理主机端口列表
    List<String> proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf);
    StringBuilder sb = new StringBuilder();
    // 遍历提取每个代理的主机名，拼接为分隔符分割的字符串
    for (String proxy : proxies) {
      sb.append(proxy.split(":")[0]).append(AmIpFilter.PROXY_HOSTS_DELIMITER);
    }
    // 移除最后一个多余的分隔符
    sb.setLength(sb.length() - 1);
    // 存入代理主机参数
    params.put(AmIpFilter.PROXY_HOSTS, sb.toString());

    // 获取HTTP/HTTPS协议前缀
    String prefix = WebAppUtils.getHttpSchemePrefix(conf);
    // 获取应用Web代理基础路径
    String proxyBase = getApplicationWebProxyBase();
    sb = new StringBuilder();
    // 拼接每个代理的完整基础URI
    for (String proxy : proxies) {
      sb.append(prefix).append(proxy).append(proxyBase)
          .append(AmIpFilter.PROXY_HOSTS_DELIMITER);
    }
    // 移除最后一个多余的分隔符
    sb.setLength(sb.length() - 1);
    // 存入代理URI基础路径参数
    params.put(AmIpFilter.PROXY_URI_BASES, sb.toString());

    // 处理RM高可用场景下的RM地址列表
    // 创建Yarn配置实例，加载yarn-site配置
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    // 获取所有RM高可用实例ID
    Collection<String> rmIds = getRmIds(yarnConf);
    if (rmIds != null) {
      List<String> urls = new ArrayList<>();
      // 根据每个RM ID获取对应Web地址
      for (String rmId : rmIds) {
        String url = getUrlByRmId(yarnConf, rmId);
        urls.add(url);
      }
      // 不为空则存入RM HA URL参数
      if (!urls.isEmpty()) {
        params.put(RM_HA_URLS, StringUtils.join(",", urls));
      }
    }

    // 向Filter容器注册过滤器
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }

  /**
   * 从配置中获取所有RM高可用实例ID
   * @param conf 配置对象
   * @return RM实例ID集合
   */
  private Collection<String> getRmIds(Configuration conf) {
    return conf.getStringCollection(YarnConfiguration.RM_HA_IDS);
  }

  /**
   * 根据RM实例ID获取对应RM Web服务地址
   * @param conf 配置对象
   * @param rmId RM实例ID
   * @return RM Web服务地址
   */
  private String getUrlByRmId(Configuration conf, String rmId) {
    // 根据是否启用HTTPS选择对应配置前缀
    String addressPropertyPrefix = YarnConfiguration.useHttps(conf) ?
        YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS :
        YarnConfiguration.RM_WEBAPP_ADDRESS;
    // 拼接RM ID后缀获取对应配置，取出地址
    String host = conf.get(HAUtil.addSuffix(addressPropertyPrefix, rmId));
    return host;
  }

  /**
   * 获取应用Web代理基础路径，从环境变量读取
   * @return 应用Web代理基础路径
   */
  @VisibleForTesting
  protected String getApplicationWebProxyBase() {
    return System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
  }
}