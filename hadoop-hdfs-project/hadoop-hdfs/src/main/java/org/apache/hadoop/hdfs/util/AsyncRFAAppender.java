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

package org.apache.hadoop.hdfs.util;

import java.io.IOException;

import org.apache.log4j.AsyncAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * 文件级注释：Log4j 1.x 环境下组合异步日志与滚动文件日志的自定义附加器
 * 
 * 该类用于解决Log4j 1.x配置中无法直接将滚动文件附加器(RollingFileAppender)包装到
 * 异步附加器(AsyncAppender)的问题，供NameNode审计日志、DataNode和NameNode指标日志
 * 使用异步滚动日志功能。迁移到Log4j2后可直接通过配置实现该功能，不再需要此类。
 * 
 * 核心职责：根据配置参数动态创建滚动文件附加器，并将其添加到异步附加器中，实现
 * 异步滚动日志输出。
 */
public class AsyncRFAAppender extends AsyncAppender {

  /**
   * 默认单个日志文件最大大小为10MB
   */
  private String maxFileSize = String.valueOf(10*1024*1024);

  /**
   * 默认保留1个备份日志文件
   */
  private int maxBackupIndex = 1;

  /**
   * 日志文件路径
   */
  private String fileName = null;

  /**
   * 日志输出格式转换模式
   */
  private String conversionPattern = null;

  /**
   * 缓冲区满时是否阻塞调用线程
   */
  private boolean blocking = true;

  /**
   * 异步日志缓冲区大小
   */
  private int bufferSize = DEFAULT_BUFFER_SIZE;

  /**
   * 持有的滚动文件附加器实例
   */
  private RollingFileAppender rollingFileAppender = null;

  /**
   * 标记滚动文件附加器是否已完成初始化添加
   */
  private volatile boolean isRollingFileAppenderAssigned = false;

  @Override
  /**
   * 追加日志事件，首次调用时完成滚动文件附加器初始化
   * @param event 待输出的日志事件
   */
  public void append(LoggingEvent event) {
    // 滚动文件附加器未初始化时先完成初始化
    if (rollingFileAppender == null) {
      appendRFAToAsyncAppender();
    }
    // 调用父类异步追加逻辑
    super.append(event);
  }

  /**
   * 同步初始化滚动文件附加器并添加到当前异步附加器中
   * 延迟懒加载初始化，确保配置参数加载完成后再创建实例
   */
  private synchronized void appendRFAToAsyncAppender() {
    // 双重检查，避免重复初始化
    if (!isRollingFileAppenderAssigned) {
      PatternLayout patternLayout;
      // 有配置格式则使用自定义格式，否则使用默认格式
      if (conversionPattern != null) {
        patternLayout = new PatternLayout(conversionPattern);
      } else {
        patternLayout = new PatternLayout();
      }
      try {
        // 创建滚动文件附加器，设置为追加写入模式
        rollingFileAppender = new RollingFileAppender(patternLayout, fileName, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // 配置滚动规则：最大备份数和单文件大小限制
      rollingFileAppender.setMaxBackupIndex(maxBackupIndex);
      rollingFileAppender.setMaxFileSize(maxFileSize);
      // 将滚动文件附加器添加到异步附加器
      this.addAppender(rollingFileAppender);
      // 标记初始化完成
      isRollingFileAppenderAssigned = true;
      // 配置异步附加器的阻塞策略和缓冲区大小
      super.setBlocking(blocking);
      super.setBufferSize(bufferSize);
    }
  }

  /**
   * 获取单个日志文件最大大小
   * @return 最大大小字符串
   */
  public String getMaxFileSize() {
    return maxFileSize;
  }

  /**
   * 设置单个日志文件最大大小
   * @param maxFileSize 最大大小字符串
   */
  public void setMaxFileSize(String maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  /**
   * 获取最大备份日志文件数量
   * @return 最大备份数量
   */
  public int getMaxBackupIndex() {
    return maxBackupIndex;
  }

  /**
   * 设置最大备份日志文件数量
   * @param maxBackupIndex 最大备份数量
   */
  public void setMaxBackupIndex(int maxBackupIndex) {
    this.maxBackupIndex = maxBackupIndex;
  }

  /**
   * 获取日志文件路径
   * @return 日志文件路径
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * 设置日志文件路径
   * @param fileName 日志文件路径
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * 获取日志输出格式转换模式
   * @return 转换模式字符串
   */
  public String getConversionPattern() {
    return conversionPattern;
  }

  /**
   * 设置日志输出格式转换模式
   * @param conversionPattern 转换模式字符串
   */
  public void setConversionPattern(String conversionPattern) {
    this.conversionPattern = conversionPattern;
  }

  /**
   * 获取缓冲区满时是否阻塞
   * @return true表示阻塞，false表示不阻塞
   */
  public boolean isBlocking() {
    return blocking;
  }

  /**
   * 设置缓冲区满时是否阻塞
   * @param blocking true表示阻塞，false表示不阻塞
   */
  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  /**
   * 获取异步缓冲区大小
   * @return 缓冲区大小
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * 设置异步缓冲区大小
   * @param bufferSize 缓冲区大小
   */
  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }
}