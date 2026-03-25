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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业结束通知工具类，负责在MapReduce作业完成后向配置的URI发送作业结束通知
 * 支持同步通知（本地作业运行器使用）和可重试的通知机制，用于第三方系统监听作业状态变更
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobEndNotifier {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobEndNotifier.class.getName());

  /**
   * 根据作业配置和作业状态创建作业结束通知对象
   * 替换URI模板中的jobId和jobStatus占位符，加载重试配置参数
   * @param conf 作业配置
   * @param status 作业最终状态
   * @return 构建完成的通知对象，如果未配置通知URI则返回null
   */
  private static JobEndStatusInfo createNotification(JobConf conf,
                                                     JobStatus status) {
    JobEndStatusInfo notification = null;
    // 获取配置的通知URI
    String uri = conf.getJobEndNotificationURI();
    if (uri != null) {
      // 读取配置的重试次数
      int retryAttempts = conf.getInt(JobContext.MR_JOB_END_RETRY_ATTEMPTS, 0);
      // 读取配置的重试间隔
      long retryInterval = conf.getInt(JobContext.MR_JOB_END_RETRY_INTERVAL, 30000);
      // 读取配置的请求超时时间
      int timeout = conf.getInt(JobContext.MR_JOB_END_NOTIFICATION_TIMEOUT,
          JobContext.DEFAULT_MR_JOB_END_NOTIFICATION_TIMEOUT);
      // 替换URI中的jobId占位符
      if (uri.contains("$jobId")) {
        uri = uri.replace("$jobId", status.getJobID().toString());
      }
      // 替换URI中的jobStatus占位符
      if (uri.contains("$jobStatus")) {
        // 根据作业运行状态转换为字符串标识
        String statusStr =
          (status.getRunState() == JobStatus.SUCCEEDED) ? "SUCCEEDED" : 
            (status.getRunState() == JobStatus.FAILED) ? "FAILED" : "KILLED";
        uri = uri.replace("$jobStatus", statusStr);
      }
      // 创建通知对象
      notification = new JobEndStatusInfo(
          uri, retryAttempts, retryInterval, timeout);
    }
    return notification;
  }

  /**
   * 执行HTTP GET通知请求，发送作业结束信息到指定URI
   * @param uri 目标通知地址
   * @param timeout 请求超时时间
   * @return HTTP响应状态码
   * @throws IOException IO异常
   * @throws URISyntaxException URI语法错误异常
   */
  private static int httpNotification(String uri, int timeout)
      throws IOException, URISyntaxException {
    // 创建HTTP客户端构建器
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    // 配置请求超时参数
    httpClientBuilder.setDefaultRequestConfig(
        RequestConfig.custom()
        .setConnectionRequestTimeout(timeout)
        .setSocketTimeout(timeout)
        .build());
    // 构建HTTP客户端
    HttpClient client = httpClientBuilder.build();
    // 创建HTTP GET请求
    HttpGet httpGet = new HttpGet(new URI(uri));
    httpGet.setHeader("Accept", "*/*");
    // 执行请求并返回响应状态码
    return client.execute(httpGet).getStatusLine().getStatusCode();
  }

  /**
   * 本地作业运行器使用的同步通知方法，不依赖线程队列，直接同步执行通知
   * 支持配置重试次数，通知失败后按间隔重试直到成功或用完重试次数
   * @param conf 作业配置
   * @param status 作业最终状态
   */
  public static void localRunnerNotification(JobConf conf, JobStatus status) {
    JobEndStatusInfo notification = createNotification(conf, status);
    if (notification != null) {
      do {
        try {
          // 发送HTTP通知
          int code = httpNotification(notification.getUri(),
              notification.getTimeout());
          if (code != 200) {
            // 响应码非200视为失败，抛出异常触发重试
            throw new IOException("Invalid response status code: " + code);
          }
          else {
            // 通知成功，跳出循环
            break;
          }
        }
        catch (IOException ioex) {
          // 记录IO异常日志
          LOG.error("Notification error [" + notification.getUri() + "]", ioex);
        }
        catch (Exception ex) {
          // 记录其他异常日志
          LOG.error("Notification error [" + notification.getUri() + "]", ex);
        }
        try {
          // 重试前等待配置的间隔
          Thread.sleep(notification.getRetryInterval());
        }
        catch (InterruptedException iex) {
          // 记录线程中断异常日志
          LOG.error("Notification retry error [" + notification + "]", iex);
        }
      } while (notification.configureForRetry()); // 判断是否还有重试次数
    }
  }

  /**
   * 作业结束通知信息容器，实现Delayed接口支持延迟调度重试
   * 存储通知URI、重试配置、超时等信息，维护剩余重试次数和延迟时间
   */
  private static class JobEndStatusInfo implements Delayed {
    private String uri;
    private int retryAttempts;
    private long retryInterval;
    private long delayTime;
    private int timeout;

    /**
     * 构造作业结束通知信息对象
     * @param uri 通知目标URI
     * @param retryAttempts 剩余重试次数
     * @param retryInterval 重试间隔
     * @param timeout HTTP请求超时时间
     */
    JobEndStatusInfo(String uri, int retryAttempts, long retryInterval,
        int timeout) {
      this.uri = uri;
      this.retryAttempts = retryAttempts;
      this.retryInterval = retryInterval;
      this.delayTime = System.currentTimeMillis();
      this.timeout = timeout;
    }

    public String getUri() {
      return uri;
    }

    public int getRetryAttempts() {
      return retryAttempts;
    }

    public long getRetryInterval() {
      return retryInterval;
    }

    public int getTimeout() {
      return timeout;
    }

    /**
     * 配置下一次重试，更新延迟时间并减少剩余重试次数
     * @return 是否可以继续重试
     */
    public boolean configureForRetry() {
      boolean retry = false;
      if (getRetryAttempts() > 0) {
        retry = true;
        // 设置下次重试的时间点
        delayTime = System.currentTimeMillis() + retryInterval;
      }
      // 减少剩余重试次数
      retryAttempts--;
      return retry;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      // 计算剩余延迟时间
      long n = this.delayTime - System.currentTimeMillis();
      return unit.convert(n, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed d) {
      // 按延迟时间排序，先执行延迟更早的任务
      return (int)(delayTime - ((JobEndStatusInfo)d).delayTime);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof JobEndStatusInfo)) {
        return false;
      }
      if (delayTime == ((JobEndStatusInfo)o).delayTime) {
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 37 * 17 + (int) (delayTime^(delayTime>>>32));
    }
      
    @Override
    public String toString() {
      return "URL: " + uri + " remaining retries: " + retryAttempts +
        " interval: " + retryInterval;
    }

  }

}