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

package org.apache.hadoop.yarn.server.webproxy;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;

import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

/**
 * YARN Web 代理工具类，负责生成经过代理的应用访问URI和路径，处理应用UI的反向代理路由构建逻辑。
 */
public class ProxyUriUtils {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(
      ProxyUriUtils.class);

  /**代理Servlet注册时使用的名称 */
  public static final String PROXY_SERVLET_NAME = "proxy";
  /**代理Servlet处理请求的基础路径 */
  public static final String PROXY_BASE = "/proxy/";
  /**代理重定向连接时添加的路径组件 */
  public static final String REDIRECT = "redirect/";
  /**代理Servlet的路径匹配规则 */
  public static final String PROXY_PATH_SPEC = PROXY_BASE+"*";
  /**表示URI已经过访问授权的查询参数名 */
  public static final String PROXY_APPROVAL_PARAM = "proxyapproved";
  
  /**
   * 对对象进行UTF-8 URL编码
   * @param o 待编码对象
   * @return 编码后的字符串
   */
  private static String uriEncode(Object o) {
    try {
      assert (o != null) : "o cannot be null";
      return URLEncoder.encode(o.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      //该异常理论上不会发生
      throw new RuntimeException("UTF-8 is not supported by this system?", e);
    }
  }
  
  /**
   * 获取应用经过代理的基础路径
   *
   * @param id 应用ID
   * @return 经过代理的应用基础路径
   */
  public static String getPath(ApplicationId id) {
    return getPath(id, false);
  }

  /**
   * 获取应用经过代理的基础路径，可指定是否包含重定向路径组件
   *
   * @param id 应用ID
   * @param redirected 是否包含重定向路径组件
   * @return 经过代理的应用基础路径
   */
  public static String getPath(ApplicationId id, boolean redirected) {
    if (id == null) {
      throw new IllegalArgumentException("Application id cannot be null ");
    }

    if (redirected) {
      return ujoin(PROXY_BASE, REDIRECT, uriEncode(id));
    } else {
      return ujoin(PROXY_BASE, uriEncode(id));
    }
  }

  /**
   * 获取应用经过代理的完整路径
   *
   * @param id 应用ID
   * @param path 应用的后续路径部分
   * @return 经过代理的应用完整路径
   */
  public static String getPath(ApplicationId id, String path) {
    return getPath(id, path, false);
  }

  /**
   * 获取应用经过代理的完整路径，可指定是否包含重定向路径组件
   *
   * @param id 应用ID
   * @param path 应用的后续路径部分
   * @param redirected 是否包含重定向路径组件
   * @return 经过代理的应用完整路径
   */
  public static String getPath(ApplicationId id, String path,
      boolean redirected) {
    if (path == null) {
      return getPath(id, redirected);
    } else {
      return ujoin(getPath(id, redirected), path);
    }
  }
  
  /**
   * 获取包含查询参数和授权标记的经过代理的完整路径
   * @param id 应用ID
   * @param path 应用路径部分
   * @param query 查询参数部分
   * @param approved 用户是否已授权访问该应用
   * @return 包含所有查询参数和授权标记的完整路径
   */
  public static String getPathAndQuery(ApplicationId id, String path, 
      String query, boolean approved) {
    StringBuilder newp = new StringBuilder();
    //拼接基础代理路径
    newp.append(getPath(id, path));
    //拼接原有查询参数，返回是否是第一个查询参数
    boolean first = appendQuery(newp, query, true);
    //如果已授权，添加授权标记查询参数
    if(approved) {
      appendQuery(newp, PROXY_APPROVAL_PARAM+"=true", first);
    }
    return newp.toString();
  }
  
  /**
   * 向StringBuilder拼接查询参数，处理问号和&分隔符
   * @param builder 路径字符串构建器
   * @param query 待拼接的查询参数
   * @param first 是否当前还没有任何查询参数
   * @return 拼接后是否仍没有查询参数（用于下一次拼接判断）
   */
  private static boolean appendQuery(StringBuilder builder, String query, 
      boolean first) {
    if(query != null && !query.isEmpty()) {
      //第一个查询参数，且没有问号，添加问号
      if(first && !query.startsWith("?")) {
        builder.append('?');
      }
      //不是第一个查询参数，且没有&，添加&分隔符
      if(!first && !query.startsWith("&")) {
        builder.append('&');
      }
      builder.append(query);
      return false;
    }
    return first;
  }
  
  /**
   * 根据原始应用URI和代理服务URI，生成最终的代理访问URI
   * @param originalUri 原始应用URI，如果为null则使用默认根路径
   * @param proxyUri 代理服务自身的URI，使用其scheme、host和port部分
   * @param id 应用ID
   * @return 生成的代理访问URI
   */
  public static URI getProxyUri(URI originalUri, URI proxyUri,
      ApplicationId id) {
    try {
      //构造代理路径，如果原始URI为空则使用默认根路径
      String path = getPath(id, originalUri == null ? "/" : originalUri.getPath());
      //构建完整代理URI，复用原始URI的查询和片段部分
      return new URI(proxyUri.getScheme(), proxyUri.getAuthority(), path,
          originalUri == null ? null : originalUri.getQuery(),
          originalUri == null ? null : originalUri.getFragment());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not proxy "+originalUri, e);
    }
  }
  
  /**
   * 从ApplicationMaster返回的无scheme地址构造完整URI
   * @param noSchemeUrl AM返回的不带scheme的URL格式
   * @return 添加了scheme的完整URI
   * @throws URISyntaxException 如果URL格式不正确
   */
  public static URI getUriFromAMUrl(String scheme, String noSchemeUrl)
      throws URISyntaxException {
      if (getSchemeFromUrl(noSchemeUrl).isEmpty()) {
        /*
         * 如果AM返回地址本身没有带scheme，使用配置的scheme（yarn.http.policy配置）
         * 如果已经带了scheme，则直接使用原地址的scheme
         */
        return new URI(scheme + noSchemeUrl);
      } else {
        return new URI(noSchemeUrl);
      }
    }

  /**
   * 从跟踪插件列表中获取第一个有效的应用跟踪URI
   * 
   * @param id 需要获取跟踪链接的应用ID
   * @param trackingUriPlugins 跟踪URI插件列表
   * @return 找到的有效跟踪URI，未找到则返回null
   * @throws URISyntaxException URI语法错误
   */
  public static URI getUriFromTrackingPlugins(ApplicationId id,
      List<TrackingUriPlugin> trackingUriPlugins)
      throws URISyntaxException {
    URI toRet = null;
    //按顺序遍历插件，返回第一个非空的跟踪URI
    for(TrackingUriPlugin plugin : trackingUriPlugins)
    {
      toRet = plugin.getTrackingUri(id);
      if (toRet != null)
      {
        return toRet;
      }
    }
    return null;
  }
  
  /**
   * 从URL中提取scheme部分
   * eg. "https://issues.apache.org/jira/browse/YARN" {@literal ->} "https"
   * @param url 待提取的URL
   * @return 提取到的scheme，未找到则返回空字符串
   */
  public static String getSchemeFromUrl(String url) {
    int index = 0;
    if (url != null) {
      index = url.indexOf("://");
    }
    if (index > 0) {
      return url.substring(0, index);
    } else {
      return "";
    }
  }

}