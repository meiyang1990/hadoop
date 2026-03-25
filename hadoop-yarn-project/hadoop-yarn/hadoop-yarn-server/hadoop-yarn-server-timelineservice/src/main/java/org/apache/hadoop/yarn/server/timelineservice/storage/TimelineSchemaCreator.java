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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时间线服务存储模式创建工具，负责初始化应用时间线信息存储结构。
 * 不同存储后端需要自行实现{@link SchemaCreator}接口，并通过yarn-site.xml配置指定实现类。
 * 本类作为命令行入口，根据配置调用对应后端的模式创建逻辑完成初始化。
 */
public class TimelineSchemaCreator extends Configured implements Tool {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineSchemaCreator.class);

  /**
   * 命令行入口，启动时间线存储模式创建流程。
   */
  public static void main(String[] args) {
    try {
      int status = ToolRunner.run(new YarnConfiguration(),
          new TimelineSchemaCreator(), args);
      System.exit(status);
    } catch (Exception e) {
      LOG.error("Error while creating Timeline Schema : ", e);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    return createTimelineSchema(args, conf);
  }

  /**
   * 根据配置加载对应后端的模式创建实现，执行时间线存储模式初始化。
   */
  @VisibleForTesting
  int createTimelineSchema(String[] args, Configuration conf) throws Exception {
    // 从配置中读取模式创建实现类名称
    String schemaCreatorClassName = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_SCHEMA_CREATOR_CLASS);
    LOG.info("Using {} for creating Timeline Service Schema ",
        schemaCreatorClassName);
    try {
      // 加载实现类
      Class<?> schemaCreatorClass = Class.forName(schemaCreatorClassName);
      // 检查类是否实现了SchemaCreator接口
      if (SchemaCreator.class.isAssignableFrom(schemaCreatorClass)) {
        // 反射实例化实现类并初始化配置
        SchemaCreator schemaCreator = (SchemaCreator) ReflectionUtils
            .newInstance(schemaCreatorClass, conf);
        // 调用后端实现创建存储模式
        schemaCreator.createTimelineSchema(args);
        return 0;
      } else {
        throw new YarnRuntimeException("Class: " + schemaCreatorClassName
            + " not instance of " + SchemaCreator.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate TimelineReader: "
          + schemaCreatorClassName, e);
    }
  }
}