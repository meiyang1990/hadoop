// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件概要：HDFS NameNode启动进度展示Servlet，通过HTTP接口以JSON格式对外提供NameNode当前启动进度信息
 * 用于监控NameNode启动过程，在NameNode启动完成前可以查询各阶段进度
 * Servlet that provides a JSON representation of the namenode's current startup
 * progress.
 */
@InterfaceAudience.Private
@SuppressWarnings("serial")
public class StartupProgressServlet extends DfsServlet {

  private static final String COUNT = "count";
  private static final String ELAPSED_TIME = "elapsedTime";
  private static final String FILE = "file";
  private static final String NAME = "name";
  private static final String DESC = "desc";
  private static final String PERCENT_COMPLETE = "percentComplete";
  private static final String PHASES = "phases";
  private static final String SIZE = "size";
  private static final String STATUS = "status";
  private static final String STEPS = "steps";
  private static final String TOTAL = "total";

  /** 当前Servlet的HTTP访问路径 */
  public static final String PATH_SPEC = "/startupProgress";

  /**
   * 处理HTTP GET请求，返回NameNode当前启动进度的JSON数据
   * @param req HTTP请求对象
   * @param resp HTTP响应对象
   * @throws IOException IO异常时抛出
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    // 设置响应内容类型为JSON，编码UTF-8
    resp.setContentType("application/json; charset=UTF-8");
    // 从Servlet上下文获取NameNode启动进度实例
    StartupProgress prog = NameNodeHttpServer.getStartupProgressFromContext(
      getServletContext());
    // 创建启动进度视图，获取当前进度的只读快照
    StartupProgressView view = prog.createView();
    // 创建JSON生成器，输出到响应流
    JsonGenerator json = new JsonFactory().createGenerator(resp.getWriter());
    try {
      // 开始写入根JSON对象
      json.writeStartObject();
      // 写入总耗时
      json.writeNumberField(ELAPSED_TIME, view.getElapsedTime());
      // 写入总体完成百分比
      json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete());
      // 开始写入启动阶段数组
      json.writeArrayFieldStart(PHASES);

      // 遍历每个启动阶段
      for (Phase phase: view.getPhases()) {
        // 开始写入当前阶段对象
        json.writeStartObject();
        // 写入阶段名称
        json.writeStringField(NAME, phase.getName());
        // 写入阶段描述
        json.writeStringField(DESC, phase.getDescription());
        // 写入阶段状态
        json.writeStringField(STATUS, view.getStatus(phase).toString());
        // 写入当前阶段完成百分比
        json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase));
        // 写入当前阶段耗时
        json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase));
        // 如果当前阶段有处理文件信息，则写入
        writeStringFieldIfNotNull(json, FILE, view.getFile(phase));
        // 如果当前阶段定义了总大小，则写入
        writeNumberFieldIfDefined(json, SIZE, view.getSize(phase));
        // 开始写入当前阶段下的步骤数组
        json.writeArrayFieldStart(STEPS);

        // 遍历当前阶段的每个步骤
        for (Step step: view.getSteps(phase)) {
          // 开始写入当前步骤对象
          json.writeStartObject();
          StepType type = step.getType();
          if (type != null) {
            // 写入步骤名称
            json.writeStringField(NAME, type.getName());
            // 写入步骤描述
            json.writeStringField(DESC, type.getDescription());
          }
          // 写入步骤已完成计数
          json.writeNumberField(COUNT, view.getCount(phase, step));
          // 如果步骤处理文件不为空，则写入
          writeStringFieldIfNotNull(json, FILE, step.getFile());
          // 如果步骤定义了处理大小，则写入
          writeNumberFieldIfDefined(json, SIZE, step.getSize());
          // 写入步骤总计数
          json.writeNumberField(TOTAL, view.getTotal(phase, step));
          // 写入步骤完成百分比
          json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase,
            step));
          // 写入步骤耗时
          json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase, step));
          // 结束写入当前步骤对象
          json.writeEndObject();
        }

        // 结束写入步骤数组
        json.writeEndArray();
        // 结束写入当前阶段对象
        json.writeEndObject();
      }

      // 结束写入启动阶段数组
      json.writeEndArray();
      // 结束写入根JSON对象
      json.writeEndObject();
    } finally {
      // 清理关闭JSON生成器，记录日志
      IOUtils.cleanupWithLogger(LOG, json);
    }
  }

  /**
   * 仅当值有效（不等于Long.MIN_VALUE）时，写入JSON数字字段
   * @param json JSON生成器
   * @param key 字段名
   * @param value 字段值
   * @throws IOException IO异常时抛出
   */
  private static void writeNumberFieldIfDefined(JsonGenerator json, String key,
      long value) throws IOException {
    if (value != Long.MIN_VALUE) {
      json.writeNumberField(key, value);
    }
  }

  /**
   * 仅当值非空时，写入JSON字符串字段
   * @param json JSON生成器
   * @param key 字段名
   * @param value 字段值
   * @throws IOException IO异常时抛出
   */
  private static void writeStringFieldIfNotNull(JsonGenerator json, String key,
      String value) throws IOException {
    if (value != null) {
      json.writeStringField(key, value);
    }
  }
}