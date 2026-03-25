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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RM_WEB_SERVICE_PATH;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.HTTPS_PREFIX;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.HTTP_PREFIX;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.glassfish.jersey.client.ClientProperties;

/**
 * 全局策略生成器(GPG)工具类，提供GPG模块通用工具能力。
 * 用于YARN联邦环境下生成全局策略时的通用功能支持。
 *
 */
public final class GPGUtils {

  // 工具类禁止实例化，隐藏构造函数
  private GPGUtils() {
  }

  /**
   * 调用远程ResourceManager的WebService接口获取信息。
   *
   * @param <T>  返回值泛型类型
   * @param webAddr 远程RM的Web地址，格式为host:port
   * @param path 接口URL路径
   * @param returnType 返回值类型
   * @param conf YARN配置对象
   * @param selectParam 过滤查询参数
   * @return 接口返回的响应实体
   */
  public static <T> T invokeRMWebService(String webAddr, String path, final Class<T> returnType,
      Configuration conf, String selectParam) {
    Client client = createJerseyClient(conf);
    T obj;

    // 从地址字符串解析得到连接用的InetSocketAddress
    InetSocketAddress socketAddress = NetUtils
        .getConnectAddress(NetUtils.createSocketAddr(webAddr));
    // 根据配置选择HTTP或HTTPS协议
    String scheme = YarnConfiguration.useHttps(conf) ? HTTPS_PREFIX : HTTP_PREFIX;
    // 拼接完整的Web服务地址
    String webAddress = scheme + socketAddress.getHostName() + ":" + socketAddress.getPort();
    WebTarget webResource = client.target(webAddress);

    // 如果有过滤参数，添加到查询参数中
    if (selectParam != null) {
      webResource = webResource.queryParam(RMWSConsts.DESELECTS, selectParam);
    }

    Response response = null;
    try {
      // 发起GET请求调用远程WebService
      response = webResource.path(RM_WEB_SERVICE_PATH).path(path)
          .request(MediaType.APPLICATION_XML).get(Response.class);
      // 响应状态正常则读取并返回实体
      if (response.getStatus() == SC_OK) {
        obj = response.readEntity(returnType);
        return obj;
      } else {
        // 响应异常抛出运行时异常
        throw new YarnRuntimeException(
            "Bad response from remote web service: " + response.getStatus());
      }
    } finally {
      // 清理资源：关闭响应
      if (response != null) {
        response.close();
      }
      // 关闭客户端
      client.close();
    }
  }

  /**
   * 调用远程ResourceManager的WebService接口获取信息，无过滤参数。
   *
   * @param <T>  返回值泛型类型
   * @param webAddr 远程RM的Web地址，格式为host:port
   * @param path 接口URL路径
   * @param returnType 返回值类型
   * @param config YARN配置对象
   * @return 接口返回的响应实体
   */
  public static <T> T invokeRMWebService(String webAddr,
      String path, final Class<T> returnType, Configuration config) {
    return invokeRMWebService(webAddr, path, returnType, config, null);
  }

  /**
   * 为所有子集群生成均匀权重，每个子集群权重均为1.0。
   *
   * @param ids 子集群ID集合
   * @return 子集群ID对应权重的映射
   */
  public static Map<SubClusterIdInfo, Float> createUniformWeights(
      Set<SubClusterId> ids) {
    Map<SubClusterIdInfo, Float> weights = new HashMap<>();
    for(SubClusterId id : ids) {
      weights.put(new SubClusterIdInfo(id), 1.0f);
    }
    return weights;
  }

  /**
   * 根据配置创建Jersey REST客户端，设置超时参数。
   *
   * @param conf YARN配置对象
   * @return 配置完成的Jersey客户端实例
   */
  public static Client createJerseyClient(Configuration conf) {
    Client client = ClientBuilder.newClient();
    // 从配置读取连接超时时间，转换为毫秒单位
    int connectTimeOut = (int) conf.getTimeDuration(YarnConfiguration.GPG_WEBAPP_CONNECT_TIMEOUT,
        YarnConfiguration.DEFAULT_GPG_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    // 设置客户端连接超时属性
    client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeOut);
    // 从配置读取读取超时时间，转换为毫秒单位
    int readTimeout = (int) conf.getTimeDuration(YarnConfiguration.GPG_WEBAPP_READ_TIMEOUT,
        YarnConfiguration.DEFAULT_GPG_WEBAPP_READ_TIMEOUT, TimeUnit.MILLISECONDS);
    // 设置客户端读取超时属性
    client.property(ClientProperties.READ_TIMEOUT, readTimeout);
    return client;
  }
}