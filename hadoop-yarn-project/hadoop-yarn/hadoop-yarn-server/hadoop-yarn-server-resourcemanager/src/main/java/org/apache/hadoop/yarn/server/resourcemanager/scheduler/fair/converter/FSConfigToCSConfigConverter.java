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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAPPING_RULE_FORMAT_JSON;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSQueueConverter.QUEUE_MAX_AM_SHARE_DISABLED;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfigurationException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.classification.VisibleForTesting;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * 将公平调度器（Fair Scheduler）配置（yarn-site和fair-scheduler.xml）转换为容量调度器（Capacity Scheduler）配置。
 * 由于功能差异，转换并非100%完美匹配，后续版本将持续完善。
 */
public class FSConfigToCSConfigConverter {
  public static final Logger LOG = LoggerFactory.getLogger(
      FSConfigToCSConfigConverter.class.getName());
  public static final String MAPPING_RULES_JSON =
      "mapping-rules.json";
  private static final String YARN_SITE_XML = "yarn-site.xml";
  private static final String CAPACITY_SCHEDULER_XML =
      "capacity-scheduler.xml";
  private static final String FAIR_SCHEDULER_XML =
      "fair-scheduler.xml";

  private Resource clusterResource;
  private boolean preemptionEnabled = false;
  private int queueMaxAppsDefault;
  private float queueMaxAMShareDefault;
  private Map<String, Integer> userMaxApps;
  private int userMaxAppsDefault;

  private boolean sizeBasedWeight = false;
  private ConversionOptions conversionOptions;
  private boolean drfUsed = false;

  private Configuration convertedYarnSiteConfig;
  private CapacitySchedulerConfiguration capacitySchedulerConfig;
  private FSConfigToCSConfigRuleHandler ruleHandler;
  private QueuePlacementConverter placementConverter;

  private OutputStream yarnSiteOutputStream;
  private OutputStream capacitySchedulerOutputStream;
  private OutputStream mappingRulesOutputStream;

  private boolean consoleMode = false;
  private boolean convertPlacementRules = true;
  private String outputDirectory;
  private boolean rulesToFile;
  private boolean usePercentages;
  private FSConfigToCSConfigConverterParams.
      PreemptionMode preemptionMode;

  /**
   * 构造配置转换器，传入规则处理器和转换选项。
   * @param ruleHandler 规则处理器
   * @param conversionOptions 转换选项
   */
  public FSConfigToCSConfigConverter(FSConfigToCSConfigRuleHandler
      ruleHandler, ConversionOptions conversionOptions) {
    this.ruleHandler = ruleHandler;
    this.conversionOptions = conversionOptions;
    this.yarnSiteOutputStream = System.out;
    this.capacitySchedulerOutputStream = System.out;
    this.placementConverter = new QueuePlacementConverter();
  }

  /**
   * 执行配置转换主流程。
   * @param params 转换参数
   * @throws Exception 转换过程中可能抛出异常
   */
  public void convert(FSConfigToCSConfigConverterParams params)
      throws Exception {
    validateParams(params);
    this.clusterResource = getClusterResource(params);
    this.convertPlacementRules = params.isConvertPlacementRules();
    this.outputDirectory = params.getOutputDirectory();
    this.rulesToFile = params.isPlacementRulesToFile();
    this.usePercentages = params.isUsePercentages();
    this.preemptionMode = params.getPreemptionMode();
    prepareOutputFiles(params.isConsole());
    loadConversionRules(params.getConversionRulesConfig());
    Configuration inputYarnSiteConfig = getInputYarnSiteConfig(params);
    handleFairSchedulerConfig(params, inputYarnSiteConfig);

    convert(inputYarnSiteConfig);
  }

  /**
   * 准备输出文件流，根据是否控制台模式选择输出目标。
   * @param console 是否控制台模式
   * @throws FileNotFoundException 输出文件不存在时抛出
   */
  private void prepareOutputFiles(boolean console)
      throws FileNotFoundException {
    if (console) {
      LOG.info("Console mode is enabled, {}, {} and {} will be only emitted " +
          "to the console!",
          YARN_SITE_XML, CAPACITY_SCHEDULER_XML, MAPPING_RULES_JSON);
      this.consoleMode = true;
      return;
    }
    File yarnSiteXmlOutput = new File(outputDirectory,
        YARN_SITE_XML);
    File schedulerXmlOutput = new File(outputDirectory,
        CAPACITY_SCHEDULER_XML);
    LOG.info("Output directory for " + YARN_SITE_XML + " and" +
        " " + CAPACITY_SCHEDULER_XML + " is: {}", outputDirectory);

    this.yarnSiteOutputStream = new FileOutputStream(yarnSiteXmlOutput);
    this.capacitySchedulerOutputStream =
        new FileOutputStream(schedulerXmlOutput);
  }

  /**
   * 验证输入参数合法性，检查必填参数。
   * @param params 转换参数
   */
  private void validateParams(FSConfigToCSConfigConverterParams params) {
    if (params.getYarnSiteXmlConfig() == null) {
      throw new PreconditionException("" + YARN_SITE_XML + " configuration " +
          "is not defined but it is mandatory!");
    } else if (params.getOutputDirectory() == null && !params.isConsole()) {
      throw new PreconditionException("Output directory configuration " +
          "is not defined but it is mandatory!");
    }
  }

  /**
   * 从参数解析集群总资源。
   * @param params 转换参数
   * @return 解析后的集群资源
   */
  private Resource getClusterResource(
      FSConfigToCSConfigConverterParams params) {
    Resource resource = null;
    if (params.getClusterResource() != null) {
      ConfigurableResource configurableResource;
      try {
        configurableResource = FairSchedulerConfiguration
            .parseResourceConfigValue(params.getClusterResource());
      } catch (AllocationConfigurationException e) {
        throw new ConversionException("Error while parsing resource.", e);
      }
      resource = configurableResource.getResource();
    }
    return resource;
  }

  /**
   * 加载用户自定义的转换规则文件。
   * @param rulesFile 规则文件路径
   * @throws IOException 读取文件失败时抛出
   */
  private void loadConversionRules(String rulesFile) throws IOException {
    if (rulesFile != null) {
      LOG.info("Reading conversion rules file from: " + rulesFile);
      ruleHandler.loadRulesFromFile(rulesFile);
    } else {
      LOG.info("Conversion rules file is not defined, " +
          "using default conversion config!");
    }

    ruleHandler.initPropertyActions();
  }

  /**
   * 加载输入的yarn-site.xml配置。
   * @param params 转换参数
   * @return 加载后的yarn-site配置
   */
  private Configuration getInputYarnSiteConfig(
      FSConfigToCSConfigConverterParams params) {
    Configuration conf = new YarnConfiguration();
    conf.addResource(new Path(params.getYarnSiteXmlConfig()));
    return conf;
  }

  /**
   * 处理公平调度器配置文件路径，从参数或输入配置中获取。
   * @param params 转换参数
   * @param conf 输入yarn-site配置
   */
  private void handleFairSchedulerConfig(
      FSConfigToCSConfigConverterParams params, Configuration conf) {
    String fairSchedulerXmlConfig = params.getFairSchedulerXmlConfig();

    // Don't override allocation file in conf yet, as it would ruin the second
    // condition here
    if (fairSchedulerXmlConfig != null) {
      LOG.info("Using explicitly defined " + FAIR_SCHEDULER_XML);
    } else if (conf.get(FairSchedulerConfiguration.ALLOCATION_FILE) != null) {
      LOG.info("Using " + FAIR_SCHEDULER_XML + " defined in " +
          YARN_SITE_XML + " by key: " +
          FairSchedulerConfiguration.ALLOCATION_FILE);
    } else {
      throw new PreconditionException("" + FAIR_SCHEDULER_XML +
          " is not defined neither in " + YARN_SITE_XML +
          "(with property: " + FairSchedulerConfiguration.ALLOCATION_FILE +
          ") nor directly with its own parameter!");
    }

    // We can now safely override allocation file in conf
    if (fairSchedulerXmlConfig != null) {
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
          params.getFairSchedulerXmlConfig());
    }
  }

  @VisibleForTesting
  void convert(Configuration inputYarnSiteConfig) throws Exception {
    // 初始化公平调度器实例，用于读取解析原有配置
    RMContext ctx = new RMContextImpl();
    PlacementManager placementManager = new PlacementManager();
    ctx.setQueuePlacementManager(placementManager);

    // Prepare a separate config for the FS instance
    // to force the use of ConfiguredYarnAuthorizer, otherwise
    // it might use that of Ranger
    Configuration fsConfig = new Configuration(inputYarnSiteConfig);
    // 开启迁移模式，跳过不必要的初始化步骤
    fsConfig.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    // 根据配置决定是否跳过终端规则检查
    fsConfig.setBoolean(FairSchedulerConfiguration.NO_TERMINAL_RULE_CHECK,
        conversionOptions.isNoRuleTerminalCheck());
    // 强制使用默认授权器，避免外部插件影响转换过程
    fsConfig.setClass(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
        ConfiguredYarnAuthorizer.class, YarnAuthorizationProvider.class);
    FairScheduler fs = new FairScheduler();
    fs.setRMContext(ctx);
    fs.init(fsConfig);

    // 检查是否使用了DRF调度策略
    drfUsed = isDrfUsed(fs);

    // 获取公平调度器分配配置，提取各类默认值
    AllocationConfiguration allocConf = fs.getAllocationConfiguration();
    queueMaxAppsDefault = allocConf.getQueueMaxAppsDefault();
    userMaxAppsDefault = allocConf.getUserMaxAppsDefault();
    userMaxApps = allocConf.getUserMaxApps();
    queueMaxAMShareDefault = allocConf.getQueueMaxAMShareDefault();

    // 初始化输出配置对象
    convertedYarnSiteConfig = new Configuration(false);
    capacitySchedulerConfig =
        new CapacitySchedulerConfiguration(new Configuration(false));

    // 转换yarn-site.xml配置
    convertYarnSiteXml(inputYarnSiteConfig);
    // 转换队列配置到capacity-scheduler.xml
    convertCapacitySchedulerXml(fs);

    // 如果需要，转换队列放置规则
    if (convertPlacementRules) {
      performRuleConversion(fs);
    }

    // 输出容量调度器配置
    if (consoleMode) {
      System.out.println("======= " + CAPACITY_SCHEDULER_XML + " =======");
    }
    capacitySchedulerConfig.writeXml(capacitySchedulerOutputStream);

    // 输出转换后的yarn-site配置
    if (consoleMode) {
      System.out.println();
      System.out.println("======= " + YARN_SITE_XML + " =======");
    }
    convertedYarnSiteConfig.writeXml(yarnSiteOutputStream);
  }

  /**
   * 转换yarn-site.xml中的调度器相关配置。
   * @param inputYarnSiteConfig 输入原始yarn-site配置
   */
  private void convertYarnSiteXml(Configuration inputYarnSiteConfig) {
    FSYarnSiteConverter siteConverter =
        new FSYarnSiteConverter();
    siteConverter.convertSiteProperties(inputYarnSiteConfig,
        convertedYarnSiteConfig, drfUsed,
        conversionOptions.isEnableAsyncScheduler(),
        usePercentages, preemptionMode);

    // 获取抢占配置和基于大小权重配置结果
    preemptionEnabled = siteConverter.isPreemptionEnabled();
    sizeBasedWeight = siteConverter.isSizeBasedWeight();

    // 检查并处理预留系统配置
    checkReservationSystem(inputYarnSiteConfig);
  }

  /**
   * 转换公平调度器队列配置到容量调度器格式。
   * @param fs 初始化完成的公平调度器实例
   */
  private void convertCapacitySchedulerXml(FairScheduler fs) {
    FSParentQueue rootQueue = fs.getQueueManager().getRootQueue();
    // 输出各类默认配置
    emitDefaultQueueMaxParallelApplications();
    emitDefaultUserMaxParallelApplications();
    emitUserMaxParallelApplications();
    emitDefaultMaxAMShare();
    emitDisablePreemptionForObserveOnlyMode();

    // 构建队列转换器
    FSQueueConverter queueConverter = FSQueueConverterBuilder.create()
        .withRuleHandler(ruleHandler)
        .withCapacitySchedulerConfig(capacitySchedulerConfig)
        .withPreemptionEnabled(preemptionEnabled)
        .withSizeBasedWeight(sizeBasedWeight)
        .withClusterResource(clusterResource)
        .withQueueMaxAMShareDefault(queueMaxAMShareDefault)
        .withQueueMaxAppsDefault(queueMaxAppsDefault)
        .withConversionOptions(conversionOptions)
        .withDrfUsed(drfUsed)
        .withPercentages(usePercentages)
        .build();

    // 递归转换整个队列层级
    queueConverter.convertQueueHierarchy(rootQueue);
    // 转换队列ACL权限配置
    emitACLs(fs);
  }

  /**
   * 执行队列放置规则转换。
   * @param fs 公平调度器实例
   * @throws IOException 写JSON输出失败时抛出
   */
  private void performRuleConversion(FairScheduler fs)
      throws IOException {
    LOG.info("Converting placement rules");

    PlacementManager placementManager =
        fs.getRMContext().getQueuePlacementManager();

    if (placementManager.getPlacementRules().size() > 0) {
      // 获取对应输出流
      mappingRulesOutputStream = getOutputStreamForJson();

      // 转换放置规则为容量调度器JSON格式
      MappingRulesDescription desc =
          placementConverter.convertPlacementPolicy(placementManager,
              ruleHandler, capacitySchedulerConfig, usePercentages);

      ObjectMapper mapper = new ObjectMapper();
      // close output stream if we write to a file, leave it open otherwise
      // 根据输出目标配置自动关闭行为
      if (!consoleMode && rulesToFile) {
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, true);
      } else {
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
      }
      ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());

      // 控制台模式且输出到文件时打印头信息
      if (consoleMode && rulesToFile) {
        System.out.println("======= " + MAPPING_RULES_JSON + " =======");
      }
      // 写入规则JSON
      writer.writeValue(mappingRulesOutputStream, desc);

      // 更新容量调度器配置指定规则格式
      capacitySchedulerConfig.setMappingRuleFormat(MAPPING_RULE_FORMAT_JSON);
      capacitySchedulerConfig.setOverrideWithQueueMappings(true);
      // 不输出到文件时直接嵌入