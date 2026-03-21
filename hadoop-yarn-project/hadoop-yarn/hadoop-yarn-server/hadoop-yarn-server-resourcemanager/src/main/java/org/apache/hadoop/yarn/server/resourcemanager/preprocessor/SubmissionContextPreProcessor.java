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

package org.apache.hadoop.yarn.server.resourcemanager.preprocessor;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * YARN RM应用提交上下文预处理器，基于客户端提交主机对应用提交上下文进行服务端预处理。
 * 从配置文件读取规则，动态修改应用提交上下文的队列、节点标签、标签等信息。
 */
public class SubmissionContextPreProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(
      SubmissionContextPreProcessor.class);
  // 默认通配规则，匹配所有未命中规则的主机
  private static final String DEFAULT_COMMANDS = "*";
  // 首次刷新配置的初始延迟时间
  private static final int INITIAL_DELAY = 1000;

  /**
   * 枚举定义支持的预处理类型，每个类型绑定对应的处理器实例。
   */
  enum ContextProp {
    // 节点标签表达式处理
    NL(new NodeLabelProcessor()),
    // 队列修改处理
    Q(new QueueProcessor()),
    // 标签添加处理
    TA(new TagAddProcessor());

    private ContextProcessor cp;
    ContextProp(ContextProcessor cp) {
      this.cp = cp;
    }
  }

  // 主机规则配置文件路径
  private String hostsFilePath;
  // 配置文件上次修改时间戳，用于判断是否需要重新加载
  private volatile long lastModified = -1;
  // 存储主机到预处理规则的映射，volatile保证并发可见性
  private volatile Map<String, Map<ContextProp, String>> hostCommands =
      new HashMap<>();
  // 定时刷新配置文件的线程池
  private ScheduledExecutorService executorService;

  /**
   * 启动预处理器，加载配置并启动定时刷新任务。
   * @param conf YARN配置对象
   */
  public void start(Configuration conf) {
    // 从配置获取规则文件路径
    this.hostsFilePath =
        conf.get(
            YarnConfiguration.RM_SUBMISSION_PREPROCESSOR_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_SUBMISSION_PREPROCESSOR_FILE_PATH);
    // 从配置获取配置文件刷新间隔
    int refreshPeriod =
        conf.getInt(
            YarnConfiguration.RM_SUBMISSION_PREPROCESSOR_REFRESH_INTERVAL_MS,
            YarnConfiguration.
                DEFAULT_RM_SUBMISSION_PREPROCESSOR_REFRESH_INTERVAL_MS);

    LOG.info("Submission Context Preprocessor enabled: file=[{}], "
            + "interval=[{}]", this.hostsFilePath, refreshPeriod);

    // 创建单线程定时线程池
    executorService = Executors.newSingleThreadScheduledExecutor();
    // 定义刷新配置任务
    Runnable refreshConf = new Runnable() {
      @Override
      public void run() {
        try {
          refresh();
        } catch (Exception ex) {
          LOG.error("Error while refreshing Submission PreProcessor file [{}]",
              hostsFilePath, ex);
        }
      }
    };
    // 根据刷新间隔选择调度方式：固定周期刷新或仅初始刷新一次
    if (refreshPeriod > 0) {
      executorService.scheduleAtFixedRate(refreshConf, INITIAL_DELAY,
          refreshPeriod, TimeUnit.MILLISECONDS);
    } else {
      executorService.schedule(refreshConf, INITIAL_DELAY,
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * 停止预处理器，关闭定时刷新线程池。
   */
  public void stop() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
    }
  }

  /**
   * 对应用提交上下文执行预处理，根据提交客户端主机匹配规则执行对应修改。
   * @param host 提交应用的客户端主机地址
   * @param applicationId 应用ID
   * @param submissionContext 应用提交上下文，将被修改
   */
  public void preProcess(String host, ApplicationId applicationId,
      ApplicationSubmissionContext submissionContext) {
    // 精确匹配主机
    Map<ContextProp, String> cMap = hostCommands.get(host);

    // 精确匹配失败，尝试正则匹配
    if (cMap == null) {
      for (Map.Entry<String, Map<ContextProp, String>> entry :
          hostCommands.entrySet()) {
        // 跳过默认通配规则，后续单独处理
        if (entry.getKey().equals(DEFAULT_COMMANDS)) {
          continue;
        }
        try {
          // 编译正则并匹配主机
          Pattern p = Pattern.compile(entry.getKey());
          Matcher m = p.matcher(host);
          if (m.find()) {
            cMap = hostCommands.get(entry.getKey());
          }
        } catch (PatternSyntaxException exception) {
          LOG.warn("Invalid regex pattern: " + entry.getKey());
        }
      }
    }
    // 仍未匹配，使用默认规则
    if (cMap == null) {
      cMap = hostCommands.get(DEFAULT_COMMANDS);
    }
    // 存在匹配规则，依次执行每个预处理操作
    if (cMap != null) {
      for (Map.Entry<ContextProp, String> entry : cMap.entrySet()) {
        entry.getKey().cp.process(host, entry.getValue(),
            applicationId, submissionContext);
      }
    }
  }

  @VisibleForTesting
  /**
   * 重新加载规则配置文件，更新内存中的规则映射。
   * @throws Exception 读取或解析配置文件出错时抛出
   */
  public void refresh() throws Exception {
    // 检查配置文件路径是否为空
    if (null == hostsFilePath || hostsFilePath.isEmpty()) {
      LOG.warn("Host list file path [{}] is empty or does not exist !!",
          hostsFilePath);
    } else {
      File hostFile = new File(hostsFilePath);
      // 检查文件是否存在且为正常文件
      if (!hostFile.exists() || !hostFile.isFile()) {
        LOG.warn("Host list file [{}] does not exist or is not a file !!",
            hostFile);
      // 检查文件是否已修改，未修改则跳过重新加载
      } else if (hostFile.lastModified() <= lastModified) {
        LOG.debug("Host list file [{}] has not been modified from last refresh",
            hostFile);
      } else {
        FileInputStream fileInputStream = new FileInputStream(hostFile);
        BufferedReader reader = null;
        // 临时存储新规则，解析成功后再替换全局引用
        Map<String, Map<ContextProp, String>> tempHostCommands =
            new HashMap<>();
        try {
          reader = new BufferedReader(new InputStreamReader(fileInputStream,
              StandardCharsets.UTF_8));
          String line;
          // 逐行读取配置文件
          while ((line = reader.readLine()) != null) {
            // Lines should start with hostname and be followed with commands.
            // Delimiter is any contiguous sequence of space or tab character.
            // Commands are of the form:
            //   <KEY>=<VALUE>
            //   where KEY can be 'NL', 'Q' or 'TA' (more can be added later)
            //   (TA stands for 'Tag Add')
            // Sample lines:
            // ...
            // host1  NL=foo   Q=b
            // host2   Q=c NL=bar
            // ...
            // 按空白字符分割行内容
            String[] commands = line.split("[ \t\n\f\r]+");
            if (commands != null && commands.length > 1) {
              String host = commands[0].trim();
              // 跳过注释行（以#开头）
              if (host.startsWith("#")) {
                // All lines starting with # is a comment
                continue;
              }
              Map<ContextProp, String> cMap = null;
              // 遍历所有规则项
              for (int i = 1; i < commands.length; i++) {
                // 按=分割规则键值对
                String[] cSplit = commands[i].split("=");
                // 格式错误则跳过当前规则项
                if (cSplit == null || cSplit.length != 2) {
                  LOG.error("No commands found for line [{}]", commands[i]);
                  continue;
                }
                // 延迟初始化规则映射
                if (cMap == null) {
                  cMap = new HashMap<>();
                }
                // 添加规则到当前主机的规则映射
                cMap.put(ContextProp.valueOf(cSplit[0]), cSplit[1]);
              }
              // 当前主机存在有效规则，保存到临时映射
              if (cMap != null && cMap.size() > 0) {
                tempHostCommands.put(host, cMap);
                LOG.info("Following commands registered for host[{}] : {}",
                    host, cMap);
              }
            }
          }
          // 更新文件修改时间戳
          lastModified = hostFile.lastModified();
        } catch (Exception ex) {
          // 解析出错，不更新规则，丢弃临时映射
          tempHostCommands = null;
          throw ex;
        } finally {
          // 解析成功，替换全局规则映射
          if (tempHostCommands != null && tempHostCommands.size() > 0) {
            hostCommands = tempHostCommands;
          }
          // 关闭输入流
          IOUtils.cleanupWithLogger(LOG, reader, fileInputStream);
        }
      }
    }
  }
}