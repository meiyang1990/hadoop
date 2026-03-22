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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.CustomJobEndNotifier;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.eclipse.jetty.util.log.Log;

/**
 * <p>This class handles job end notification. Submitters of jobs can choose to
 * be notified of the end of a job by supplying a URL to which a connection
 * will be established.
 * <ul><li> The URL connection is fire and forget by default.</li> <li>
 * User can specify number of retry attempts and a time interval at which to
 * attempt retries</li><li>
 * Cluster administrators can set final parameters to set maximum number of
 * tries (0 would disable job end notification) and max time interval and a
 * proxy if needed</li><li>
 * The URL may contain sentinels which will be replaced by jobId and jobStatus 
 * (eg. SUCCEEDED/KILLED/FAILED) </li> </ul>
 */
/**
 * MapReduce作业结束通知处理器，负责在作业完成后向用户指定URL发送完成通知
 * 支持自定义通知实现、重试机制、代理配置，支持URL中动态替换作业ID和作业状态
 */
public class JobEndNotifier implements Configurable {
  private static final String JOB_ID = "$jobId";
  private static final String JOB_STATUS = "$jobStatus";

  private Configuration conf;
  protected String userUrl;
  protected String proxyConf;
  protected int numTries; //Number of tries to attempt notification
  protected int waitInterval; //Time (ms) to wait between retrying notification
  protected int timeout; // Timeout (ms) on the connection and notification
  protected URL urlToNotify; //URL to notify read from the config
  protected Proxy proxyToUse = Proxy.NO_PROXY; //Proxy to use for notification
  // A custom notifier implementation
  // (see org.apache.hadoop.mapreduce.CustomJobEndNotifier)
  private String customJobEndNotifierClassName;

  /**
   * 从配置中解析作业结束通知相关参数，包括重试次数、重试间隔、超时时间、通知URL、代理配置和自定义通知实现类
   * @param conf 作业配置对象
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    
    // 计算实际重试次数，取用户配置+1和集群管理员配置的最大尝试次数中的较小值
    numTries = Math.min(
      conf.getInt(MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS, 0) + 1
      , conf.getInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS, 1)
    );
    // 计算实际重试间隔，取用户配置和集群管理员配置的最大间隔中的较小值
    waitInterval = Math.min(
    conf.getInt(MRJobConfig.MR_JOB_END_RETRY_INTERVAL, 5000)
    , conf.getInt(MRJobConfig.MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL, 5000)
    );
    // 重试间隔非法时设置默认值
    waitInterval = (waitInterval < 0) ? 5000 : waitInterval;

    // 读取连接超时配置
    timeout = conf.getInt(JobContext.MR_JOB_END_NOTIFICATION_TIMEOUT,
        JobContext.DEFAULT_MR_JOB_END_NOTIFICATION_TIMEOUT);

    // 读取用户配置的通知URL
    userUrl = conf.get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL);

    // 读取代理配置
    proxyConf = conf.get(MRJobConfig.MR_JOB_END_NOTIFICATION_PROXY);

    // 读取自定义通知实现类名
    customJobEndNotifierClassName = StringUtils.stripToNull(
        conf.get(MRJobConfig.MR_JOB_END_NOTIFICATION_CUSTOM_NOTIFIER_CLASS));

    //Configure the proxy to use if its set. It should be set like
    //proxyType@proxyHostname:port
    // 如果配置了代理则解析代理参数并初始化代理对象
    if(proxyConf != null && !proxyConf.equals("") &&
         proxyConf.lastIndexOf(":") != -1) {
      // 解析代理类型位置
      int typeIndex = proxyConf.indexOf("@");
      Proxy.Type proxyType = Proxy.Type.HTTP;
      // 判断是否为SOCKS代理
      if(typeIndex != -1 &&
        proxyConf.substring(0, typeIndex).compareToIgnoreCase("socks") == 0) {
        proxyType = Proxy.Type.SOCKS;
      }
      // 提取代理主机名
      String hostname = proxyConf.substring(typeIndex + 1,
        proxyConf.lastIndexOf(":"));
      // 提取端口字符串
      String portConf = proxyConf.substring(proxyConf.lastIndexOf(":") + 1);
      try {
        // 解析端口并创建代理对象
        int port = Integer.parseInt(portConf);
        proxyToUse = new Proxy(proxyType,
          new InetSocketAddress(hostname, port));
        Log.getLog().info("Job end notification using proxy type \""
            + proxyType + "\" hostname \"" + hostname + "\" and port \"" + port
            + "\"");
      } catch(NumberFormatException nfe) {
        // 端口解析失败，不使用代理
        Log.getLog().warn("Job end notification couldn't parse configured"
            + "proxy's port " + portConf + ". Not going to use a proxy");
      }
    }

  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * 执行一次通知尝试，根据是否配置自定义通知类选择对应通知方式
   * @return 通知成功返回true，失败返回false
   */
  protected boolean notifyURLOnce() {
    if (customJobEndNotifierClassName == null) {
      return notifyViaBuiltInNotifier();
    } else {
      return notifyViaCustomNotifier();
    }
  }

  /**
   * 使用内置HTTP方式发送作业结束通知
   * @return 通知成功返回true，失败返回false
   */
  private boolean notifyViaBuiltInNotifier() {
    boolean success = false;
    try {
      Log.getLog().info("Job end notification trying " + urlToNotify);
      // 打开URL连接，使用配置好的代理
      HttpURLConnection conn =
        (HttpURLConnection) urlToNotify.openConnection(proxyToUse);
      // 设置连接超时
      conn.setConnectTimeout(timeout);
      // 设置读取超时
      conn.setReadTimeout(timeout);
      // 禁止用户交互
      conn.setAllowUserInteraction(false);
      // 响应码不是OK则记录警告日志
      if(conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        Log.getLog().warn("Job end notification to " + urlToNotify
            + " failed with code: " + conn.getResponseCode() + " and message \""
            + conn.getResponseMessage() + "\"");
      }
      else {
        // 通知成功
        success = true;
        Log.getLog().info("Job end notification to " + urlToNotify
            + " succeeded");
      }
    } catch(IOException ioe) {
      // IO异常，通知失败
      Log.getLog().warn("Job end notification to " + urlToNotify + " failed",
          ioe);
    }
    return success;
  }

  /**
   * 使用用户自定义实现类发送作业结束通知
   * @return 通知成功返回true，失败返回false
   */
  private boolean notifyViaCustomNotifier() {
    try {
      Log.getLog().info("Will be using " + customJobEndNotifierClassName
                        + " for Job end notification");

      // 加载自定义通知类并转换为CustomJobEndNotifier子类
      final Class<? extends CustomJobEndNotifier> customJobEndNotifierClass =
              Class.forName(customJobEndNotifierClassName)
                      .asSubclass(CustomJobEndNotifier.class);
      // 通过反射创建自定义通知实例
      final CustomJobEndNotifier customJobEndNotifier =
              customJobEndNotifierClass.getDeclaredConstructor().newInstance();

      // 调用自定义通知方法执行通知
      boolean success = customJobEndNotifier.notifyOnce(urlToNotify, conf);
      if (success) {
        Log.getLog().info("Job end notification to " + urlToNotify
                          + " succeeded");
      } else {
        Log.getLog().warn("Job end notification to " + urlToNotify
                          + " failed");
      }
      return success;
    } catch (Exception e) {
      // 任何异常都判定为通知失败
      Log.getLog().warn("Job end notification to " + urlToNotify
                        + " failed", e);
      return false;
    }
  }

  /**
   * 对外入口方法，作业完成后调用该方法发送结束通知
   * @param jobReport 作业报告对象，包含作业ID和最终状态信息
   * @throws InterruptedException 重试等待过程中被中断时抛出
   */
  public void notify(JobReport jobReport)
    throws InterruptedException {

    // 替换URL中的作业ID占位符
    if (userUrl.contains(JOB_ID)) {
      userUrl = userUrl.replace(JOB_ID, jobReport.getJobId().toString());
    }
    // 替换URL中的作业状态占位符
    if (userUrl.contains(JOB_STATUS)) {
      userUrl = userUrl.replace(JOB_STATUS, jobReport.getJobState().toString());
    }

    // Create the URL, ensure sanity
    // 解析替换后的URL
    try {
      urlToNotify = new URL(userUrl);
    } catch (MalformedURLException mue) {
      // URL非法，终止通知
      Log.getLog().warn("Job end notification couldn't parse " + userUrl, mue);
      return;
    }

    // Send notification
    // 循环重试通知直到成功或用完重试次数
    boolean success = false;
    while (numTries-- > 0 && !success) {
      Log.getLog().info("Job end notification attempts left " + numTries);
      success = notifyURLOnce();
      // 失败则等待后重试
      if (!success) {
        Thread.sleep(waitInterval);
      }
    }
    // 最终通知失败记录警告日志
    if (!success) {
      Log.getLog().warn("Job end notification failed to notify : "
          + urlToNotify);
    } else {
      // 通知成功记录信息日志
      Log.getLog().info("Job end notification succeeded for "
          + jobReport.getJobId());
    }
  }
}