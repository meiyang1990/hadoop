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
package org.apache.hadoop.mapreduce.v2.util;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;
import java.util.Iterator;

import static org.apache.hadoop.http.HttpConfig.Policy;

/**
 * MapReduce Web应用工具类，提供YARN和JobHistoryServer相关Web地址、HTTP协议处理能力，
 * 用于构建MapReduce作业和应用的Web访问链接，支撑Web UI的链接生成功能。
 */
@Private
@Evolving
public class MRWebAppUtil {
  /** 地址分隔器，按冒号分割主机端口 */
  private static final Splitter ADDR_SPLITTER = Splitter.on(':').trimResults();
  /** 字符串拼接器 */
  private static final Joiner JOINER = Joiner.on("");

  /** YARN的HTTP策略缓存 */
  private static Policy httpPolicyInYarn;
  /** JobHistoryServer的HTTP策略缓存 */
  private static Policy httpPolicyInJHS;

  /**
   * 初始化工具类，从配置加载YARN和JHS的HTTP策略。
   * @param conf Hadoop配置对象
   */
  public static void initialize(Configuration conf) {
    setHttpPolicyInYARN(conf.get(
            YarnConfiguration.YARN_HTTP_POLICY_KEY,
            YarnConfiguration.YARN_HTTP_POLICY_DEFAULT));
    setHttpPolicyInJHS(conf.get(JHAdminConfig.MR_HS_HTTP_POLICY,
            JHAdminConfig.DEFAULT_MR_HS_HTTP_POLICY));
  }
  
  /**
   * 设置JHS的HTTP策略。
   * @param policy 策略字符串
   */
  private static void setHttpPolicyInJHS(String policy) {
    MRWebAppUtil.httpPolicyInJHS = Policy.fromString(policy);
  }
  
  /**
   * 设置YARN的HTTP策略。
   * @param policy 策略字符串
   */
  private static void setHttpPolicyInYARN(String policy) {
    MRWebAppUtil.httpPolicyInYarn = Policy.fromString(policy);
  }

  /**
   * 获取JHS的HTTP策略。
   * @return HTTP策略对象
   */
  public static Policy getJHSHttpPolicy() {
    return MRWebAppUtil.httpPolicyInJHS;
  }

  /**
   * 获取YARN的HTTP策略。
   * @return HTTP策略对象
   */
  public static Policy getYARNHttpPolicy() {
    return MRWebAppUtil.httpPolicyInYarn;
  }

  /**
   * 获取YARN Web应用的协议前缀（http://或https://）。
   * @return 协议前缀字符串
   */
  public static String getYARNWebappScheme() {
    return httpPolicyInYarn == HttpConfig.Policy.HTTPS_ONLY ? "https://"
        : "http://";
  }

  /**
   * 获取JHS Web应用的协议前缀（http://或https://）。
   * @param conf Hadoop配置对象
   * @return 协议前缀字符串
   */
  public static String getJHSWebappScheme(Configuration conf) {
    setHttpPolicyInJHS(conf.get(JHAdminConfig.MR_HS_HTTP_POLICY,
        JHAdminConfig.DEFAULT_MR_HS_HTTP_POLICY));
    return httpPolicyInJHS == HttpConfig.Policy.HTTPS_ONLY ? "https://"
        : "http://";
  }
  
  /**
   * 设置不带协议前缀的JHS Web应用地址到配置。
   * 根据HTTP策略选择对应配置项存储地址。
   * @param conf Hadoop配置对象
   * @param hostAddress 不带协议的地址字符串
   */
  public static void setJHSWebappURLWithoutScheme(Configuration conf,
      String hostAddress) {
    if (httpPolicyInJHS == Policy.HTTPS_ONLY) {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS, hostAddress);
    } else {
      conf.set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS, hostAddress);
    }
  }
  
  /**
   * 获取不带协议前缀的JHS Web应用地址。
   * @param conf Hadoop配置对象
   * @return 不带协议的地址字符串
   */
  public static String getJHSWebappURLWithoutScheme(Configuration conf) {
    if (httpPolicyInJHS == Policy.HTTPS_ONLY) {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS);
    }
  }
  
  /**
   * 获取带协议前缀的完整JHS Web应用地址。
   * @param conf Hadoop配置对象
   * @return 完整JHS访问地址
   */
  public static String getJHSWebappURLWithScheme(Configuration conf) {
    return getJHSWebappScheme(conf) + getJHSWebappURLWithoutScheme(conf);
  }
  
  /**
   * 获取JHS Web服务绑定地址。
   * @param conf Hadoop配置对象
   * @return JHS Web服务绑定套接字地址
   */
  public static InetSocketAddress getJHSWebBindAddress(Configuration conf) {
    if (httpPolicyInJHS == Policy.HTTPS_ONLY) {
      return conf.getSocketAddr(
          JHAdminConfig.MR_HISTORY_BIND_HOST,
          JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT);
    } else {
      return conf.getSocketAddr(
          JHAdminConfig.MR_HISTORY_BIND_HOST,
          JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS,
          JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT);
    }
  }
  
  /**
   * 获取JHS上指定应用的作业详情页地址（不带协议前缀）。
   * @param conf Hadoop配置对象
   * @param appId YARN应用ID
   * @return 不带协议的作业详情页路径
   * @throws UnknownHostException 获取本地主机名失败时抛出
   */
  public static String getApplicationWebURLOnJHSWithoutScheme(Configuration conf,
      ApplicationId appId)
      throws UnknownHostException {
    // 获取JHS Web地址
    String addr = getJHSWebappURLWithoutScheme(conf);
    String port;
    try{
      // 分割地址提取端口
      Iterator<String> it = ADDR_SPLITTER.split(addr).iterator();
      it.next(); // 跳过绑定主机
      port = it.next();
    } catch(NoSuchElementException e) {
      throw new IllegalArgumentException("MapReduce JobHistory WebApp Address"
        + " does not contain a valid host:port authority: " + addr);
    }
    // 从服务地址配置中提取主机名
    addr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS);
    String host = ADDR_SPLITTER.split(addr).iterator().next();
    String hsAddress = JOINER.join(host, ":", port);
    // 构建套接字地址
    InetSocketAddress address = NetUtils.createSocketAddr(
      hsAddress, getDefaultJHSWebappPort(),
      getDefaultJHSWebappURLWithoutScheme());
    StringBuilder sb = new StringBuilder();
    // 如果绑定任意地址或本地回环，使用本地主机名
    if (address.getAddress() != null &&
        (address.getAddress().isAnyLocalAddress() ||
         address.getAddress().isLoopbackAddress())) {
      sb.append(InetAddress.getLocalHost().getCanonicalHostName());
    } else {
      sb.append(address.getHostName());
    }
    // 拼接端口和作业路径
    sb.append(":").append(address.getPort());
    sb.append("/jobhistory/job/");
    // 转换YARN应用ID为MapReduce作业ID
    JobID jobId = TypeConverter.fromYarn(appId);
    sb.append(jobId.toString());
    return sb.toString();
  }
  
  /**
   * 获取JHS上指定应用的完整作业详情页URL（带协议前缀）。
   * @param conf Hadoop配置对象
   * @param appId YARN应用ID
   * @return 完整作业详情URL
   * @throws UnknownHostException 获取本地主机名失败时抛出
   */
  public static String getApplicationWebURLOnJHSWithScheme(Configuration conf,
      ApplicationId appId) throws UnknownHostException {
    return getJHSWebappScheme(conf)
        + getApplicationWebURLOnJHSWithoutScheme(conf, appId);
  }

  /**
   * 获取JHS Web应用默认端口，根据HTTP协议返回对应端口。
   * @return 默认端口号
   */
  private static int getDefaultJHSWebappPort() {
    return httpPolicyInJHS == Policy.HTTPS_ONLY ?
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT:
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT;
  }
  
  /**
   * 获取默认的JHS Web地址（不带协议前缀）。
   * @return 默认地址字符串
   */
  private static String getDefaultJHSWebappURLWithoutScheme() {
    return httpPolicyInJHS == Policy.HTTPS_ONLY ?
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS :
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS;
  }

  /**
   * 获取MR ApplicationMaster Web界面的协议前缀。
   * @param conf Hadoop配置对象
   * @return 协议前缀（http://或https://）
   */
  public static String getAMWebappScheme(Configuration conf) {
    return conf.getBoolean(
        MRJobConfig.MR_AM_WEBAPP_HTTPS_ENABLED,
        MRJobConfig.DEFAULT_MR_AM_WEBAPP_HTTPS_ENABLED)
        ? "https://" : "http://";
  }
}