// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.util.function.Consumer;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * 公平调度器(FS)配置转容量调度器(CS)配置转换器入口类
 * 负责启动配置转换流程，处理异常和程序退出
 *
 */
public final class FSConfigToCSConfigConverterMain {
  private FSConfigToCSConfigConverterMain() {
    // 禁止实例化工具类
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(FSConfigToCSConfigConverterMain.class);
  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");
  // 程序退出处理器，默认调用System.exit，可用于单元测试替换
  private static Consumer<Integer> exitFunction = System::exit;

  /**
   * 配置转换程序主入口
   * @param args 命令行参数
   */
  public static void main(String[] args) {
    try {
      // 创建命令行参数处理器
      FSConfigToCSConfigArgumentHandler fsConfigConversionArgumentHandler =
          new FSConfigToCSConfigArgumentHandler();
      // 解析参数并执行转换，获取退出码
      int exitCode =
          fsConfigConversionArgumentHandler.parseAndConvert(args);
      // 转换失败打印错误日志
      if (exitCode != 0) {
        LOG.error(FATAL,
            "Error while starting FS configuration conversion, " +
                "see previous error messages for details!");
      }

      // 按退出码退出程序
      exitFunction.accept(exitCode);
    } catch (Throwable t) {
      // 捕获未处理异常，打印致命错误日志后退出
      LOG.error(FATAL,
          "Error while starting FS configuration conversion!", t);
      exitFunction.accept(-1);
    }
  }

  /**
   * 替换退出处理器，用于单元测试
   * @param exitFunc 自定义退出处理器
   */
  @VisibleForTesting
  static void setExit(Consumer<Integer> exitFunc) {
    exitFunction = exitFunc;
  }
}