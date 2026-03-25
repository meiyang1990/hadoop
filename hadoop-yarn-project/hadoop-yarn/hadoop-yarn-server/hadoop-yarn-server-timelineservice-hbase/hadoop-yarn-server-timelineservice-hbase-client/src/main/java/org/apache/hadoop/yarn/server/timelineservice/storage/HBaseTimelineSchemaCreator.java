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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.domain.DomainTableRW;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级：时间线服务HBase后端存储模式创建工具，负责创建时间线服务所需的所有HBase表结构。
 * This creates the schema for a hbase based backend for storing application
 * timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HBaseTimelineSchemaCreator implements SchemaCreator {
  public HBaseTimelineSchemaCreator() {
  }

  final static String NAME = HBaseTimelineSchemaCreator.class.getSimpleName();
  private static final Logger LOG =
      LoggerFactory.getLogger(HBaseTimelineSchemaCreator.class);
  // 跳过已存在表命令行选项短名
  private static final String SKIP_EXISTING_TABLE_OPTION_SHORT = "s";
  // 应用指标TTL命令行选项短名
  private static final String APP_METRICS_TTL_OPTION_SHORT = "ma";
  // 子应用指标TTL命令行选项短名
  private static final String SUB_APP_METRICS_TTL_OPTION_SHORT = "msa";
  // 应用表名命令行选项短名
  private static final String APP_TABLE_NAME_SHORT = "a";
  // 子应用表名命令行选项短名
  private static final String SUB_APP_TABLE_NAME_SHORT = "sa";
  // 应用到流映射表名命令行选项短名
  private static final String APP_TO_FLOW_TABLE_NAME_SHORT = "a2f";
  // 实体指标TTL命令行选项短名
  private static final String ENTITY_METRICS_TTL_OPTION_SHORT = "me";
  // 实体表名命令行选项短名
  private static final String ENTITY_TABLE_NAME_SHORT = "e";
  // 帮助命令行选项短名
  private static final String HELP_SHORT = "h";
  // 创建表命令行选项短名
  private static final String CREATE_TABLES_SHORT = "c";

  /**
   * 方法级：处理命令行参数，触发时间线服务HBase表schema创建流程。
   * @param args 命令行参数数组
   * @throws Exception 创建过程中抛出的任意异常
   */
  public void createTimelineSchema(String[] args) throws Exception {

    LOG.info("Starting the schema creation");
    // 初始化HBase配置，基于Yarn配置加载
    Configuration hbaseConf =
        HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(
            new YarnConfiguration());
    // 处理GenericOptionsParser支持的-D参数，提取剩余用户参数
    String[] otherArgs = new GenericOptionsParser(hbaseConf, args)
        .getRemainingArgs();

    // 解析用户命令行参数
    CommandLine commandLine = parseArgs(otherArgs);

    // 帮助选项优先级最高
    if (commandLine.hasOption(HELP_SHORT)) {
      // -help option has the highest precedence
      printUsage();
    } else if (commandLine.hasOption(CREATE_TABLES_SHORT)) {
      // 获取实体表名参数并覆盖配置
      String entityTableName = commandLine.getOptionValue(
          ENTITY_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(entityTableName)) {
        hbaseConf.set(EntityTableRW.TABLE_NAME_CONF_NAME, entityTableName);
      }
      // 获取实体表指标TTL参数并设置
      String entityTableMetricsTTL = commandLine.getOptionValue(
          ENTITY_METRICS_TTL_OPTION_SHORT);
      if (StringUtils.isNotBlank(entityTableMetricsTTL)) {
        int entityMetricsTTL = Integer.parseInt(entityTableMetricsTTL);
        new EntityTableRW().setMetricsTTL(entityMetricsTTL, hbaseConf);
      }
      // 获取应用到流映射表名参数并覆盖配置
      String appToflowTableName = commandLine.getOptionValue(
          APP_TO_FLOW_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(appToflowTableName)) {
        hbaseConf.set(
            AppToFlowTableRW.TABLE_NAME_CONF_NAME, appToflowTableName);
      }
      // 获取应用表名参数并覆盖配置
      String applicationTableName = commandLine.getOptionValue(
          APP_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(applicationTableName)) {
        hbaseConf.set(ApplicationTableRW.TABLE_NAME_CONF_NAME,
            applicationTableName);
      }
      // 获取应用表指标TTL参数并设置
      String applicationTableMetricsTTL = commandLine.getOptionValue(
          APP_METRICS_TTL_OPTION_SHORT);
      if (StringUtils.isNotBlank(applicationTableMetricsTTL)) {
        int appMetricsTTL = Integer.parseInt(applicationTableMetricsTTL);
        new ApplicationTableRW().setMetricsTTL(appMetricsTTL, hbaseConf);
      }

      // 获取子应用表名参数并覆盖配置
      String subApplicationTableName = commandLine.getOptionValue(
          SUB_APP_TABLE_NAME_SHORT);
      if (StringUtils.isNotBlank(subApplicationTableName)) {
        hbaseConf.set(SubApplicationTableRW.TABLE_NAME_CONF_NAME,
            subApplicationTableName);
      }
      // 获取子应用表指标TTL参数并设置
      String subApplicationTableMetricsTTL = commandLine
          .getOptionValue(SUB_APP_METRICS_TTL_OPTION_SHORT);
      if (StringUtils.isNotBlank(subApplicationTableMetricsTTL)) {
        int subAppMetricsTTL = Integer.parseInt(subApplicationTableMetricsTTL);
        new SubApplicationTableRW().setMetricsTTL(subAppMetricsTTL, hbaseConf);
      }

      // 获取是否跳过已有表参数，执行所有表创建
      final boolean skipExisting = commandLine.hasOption(
          SKIP_EXISTING_TABLE_OPTION_SHORT);
      createAllSchemas(hbaseConf, skipExisting);
    } else {
      // 未指定create选项，打印帮助信息
      printUsage();
    }
  }

  /**
   * Parse command-line arguments.
   *
   * @param args
   *          command line arguments passed to program.
   * @return parsed command line.
   * @throws ParseException
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();

    // 添加帮助选项
    Option o = new Option(HELP_SHORT, "help", false, "print help information");
    o.setRequired(false);
    options.addOption(o);

    // 添加创建表选项
    o = new Option(CREATE_TABLES_SHORT, "create", false,
        "a mandatory option to create hbase tables");
    o.setRequired(false);
    options.addOption(o);

    // 添加实体表名选项
    o = new Option(ENTITY_TABLE_NAME_SHORT, "entityTableName", true,
        "entity table name");
    o.setArgName("entityTableName");
    o.setRequired(false);
    options.addOption(o);

    // 添加实体表指标TTL选项
    o = new Option(ENTITY_METRICS_TTL_OPTION_SHORT, "entityMetricsTTL", true,
        "TTL for metrics column family");
    o.setArgName("entityMetricsTTL");
    o.setRequired(false);
    options.addOption(o);

    // 添加应用到流映射表名选项
    o = new Option(APP_TO_FLOW_TABLE_NAME_SHORT, "appToflowTableName", true,
        "app to flow table name");
    o.setArgName("appToflowTableName");
    o.setRequired(false);
    options.addOption(o);

    // 添加应用表名选项
    o = new Option(APP_TABLE_NAME_SHORT, "applicationTableName", true,
        "application table name");
    o.setArgName("applicationTableName");
    o.setRequired(false);
    options.addOption(o);

    // 添加应用表指标TTL选项
    o = new Option(APP_METRICS_TTL_OPTION_SHORT, "applicationMetricsTTL", true,
        "TTL for metrics column family");
    o.setArgName("applicationMetricsTTL");
    o.setRequired(false);
    options.addOption(o);

    // 添加子应用表名选项
    o = new Option(SUB_APP_TABLE_NAME_SHORT, "subApplicationTableName", true,
        "subApplication table name");
    o.setArgName("subApplicationTableName");
    o.setRequired(false);
    options.addOption(o);

    // 添加子应用表指标TTL选项
    o = new Option(SUB_APP_METRICS_TTL_OPTION_SHORT, "subApplicationMetricsTTL",
        true, "TTL for metrics column family");
    o.setArgName("subApplicationMetricsTTL");
    o.setRequired(false);
    options.addOption(o);

    // 添加跳过已有表选项
    // Options without an argument
    // No need to set arg name since we do not need an argument here
    o = new Option(SKIP_EXISTING_TABLE_OPTION_SHORT, "skipExistingTable",
        false, "skip existing Hbase tables and continue to create new tables");
    o.setRequired(false);
    options.addOption(o);

    CommandLineParser parser = new PosixParser();
    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (Exception e) {
      LOG.error("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }

    return commandLine;
  }

  /**
   * 方法级：打印命令行使用帮助信息。
   */
  private static void printUsage() {
    StringBuilder usage = new StringBuilder("Command Usage: \n");
    usage
        .append("TimelineSchemaCreator [-help] Display help info"
            + " for all commands. Or\n")
        .append("TimelineSchemaCreator -create [OPTIONAL_OPTIONS]" +
            " Create hbase tables.\n\n")
        .append("The Optional options for creating tables include: \n")
        .append("[-entityTableName <Entity Table Name>] " +
            "The name of the Entity table\n")
        .append("[-entityMetricsTTL <Entity Table Metrics TTL>]" +
            " TTL for metrics in the Entity table\n")
        .append("[-appToflowTableName <AppToflow Table Name>]" +
            " The name of the AppToFlow table\n")
        .append("[-applicationTableName <Application Table Name>]" +
            " The name of the Application table\n")
        .append("[-applicationMetricsTTL <Application Table Metrics TTL>]" +
            " TTL for metrics in the Application table\n")
        .append("[-subApplicationTableName <SubApplication Table Name>]" +
            " The name of the SubApplication table\n")
        .append("[-subApplicationMetricsTTL "
            + " <SubApplication Table Metrics TTL>]" +
            " TTL for metrics in the SubApplication table\n")
        .append("[-skipExistingTable] Whether to skip existing" +
            " hbase tables\n");
    System.out.println(usage.toString());
  }

  /**
   * Create all table schemas and log success or exception if failed.
   * @param hbaseConf the hbase configuration to create tables with
   * @param skipExisting whether to skip existing hbase tables
   */
  private static void createAllSchemas(Configuration hbaseConf,
      boolean skipExisting) {
    List<Exception> exceptions = new ArrayList<>();
    try {
      if (skipExisting) {
        LOG.info("Will skip existing tables and continue on htable creation "
            + "exceptions!");
      }
      createAllTables(hbaseConf, skipExisting);
      LOG.info("Successfully created HBase schema. ");
    } catch (IOException e) {
      LOG.error("Error in creating hbase tables: ", e);
      exceptions.add(e);
    }

    if (exceptions.size() > 0) {
      LOG.warn("Schema creation finished with the following exceptions");
      for (Exception e : exceptions) {
        LOG.warn(e.getMessage());
      }
      System.exit(-1);
    } else {
      LOG.info("Schema creation finished successfully");
    }
  }

  @VisibleForTesting
  /**
   * 方法级：逐个创建时间线服务所需的所有HBase表，支持跳过已存在表。
   * @param hbaseConf HBase配置
   * @param skipExisting 是否跳过已存在表
   * @throws IOException 创建过程中发生IO异常
   */
  public static void createAllTables(Configuration hbaseConf,
      boolean skipExisting) throws IOException {

    Connection conn = null;
    try {
      // 创建HBase连接
      conn = ConnectionFactory.createConnection(hbaseConf);
      // 获取HBase管理员客户端
      Admin admin = conn.getAdmin();
      if (admin == null) {
        throw new IOException("Cannot create table since admin is null");
      }
      // 创建实体表
      try {
        new EntityTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      // 创建应用到流映射表
      try {
        new AppToFlowTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      // 创建应用表
      try {
        new ApplicationTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      // 创建流运行表
      try {
        new FlowRunTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      // 创建流活动表
      try {
        new FlowActivityTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      // 创建子应用表
      try {
        new SubApplicationTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
      // 创建域表
      try {
        new DomainTableRW().createTable(admin, hbaseConf);
      } catch (IOException e) {
        if (skipExisting) {
          LOG.warn("Skip and continue on: " + e.getMessage());
        } else {
          throw e;
        }
      }
    } finally {
      // 关闭HBase连接释放资源
      if (conn != null) {
        conn.close();
      }
    }
  }


}