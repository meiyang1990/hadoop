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

import java.io.File;
import java.util.function.Supplier;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级注释：公平调度器(FS)配置转容量调度器(CS)配置工具的命令行参数处理器
 * 核心职责：解析命令行参数，验证参数合法性，调用配置转换器执行转换
 */
public class FSConfigToCSConfigArgumentHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSConfigToCSConfigArgumentHandler.class);

  // 参数冲突异常信息模板
  private static final String ALREADY_CONTAINS_EXCEPTION_MSG =
      "The %s (provided with %s|%s arguments) contains " +
          "the %s provided with the %s|%s options.";
  // 输出目录已有目标文件异常信息模板
  private static final String ALREADY_CONTAINS_FILE_EXCEPTION_MSG =
      "The %s %s (provided with %s|%s arguments) already contains a file " +
          "or directory named %s which will be the output of the conversion!";

  private FSConfigToCSConfigRuleHandler ruleHandler;
  private FSConfigToCSConfigConverterParams converterParams;
  private ConversionOptions conversionOptions;
  private ConvertedConfigValidator validator;

  // 转换器工厂，支持测试时注入自定义转换器
  private Supplier<FSConfigToCSConfigConverter>
      converterFunc = this::getConverter;

  /**
   * 默认构造函数，初始化转换选项和配置验证器
   */
  public FSConfigToCSConfigArgumentHandler() {
    this.conversionOptions = new ConversionOptions(new DryRunResultHolder(),
        false);
    this.validator = new ConvertedConfigValidator();
  }

  @VisibleForTesting
  FSConfigToCSConfigArgumentHandler(ConversionOptions conversionOptions,
      ConvertedConfigValidator validator) {
    this.conversionOptions = conversionOptions;
    this.validator = validator;
  }

  /**
   * CLI命令行选项枚举，定义所有支持的命令行参数
   */
  public enum CliOption {
    YARN_SITE("yarn-site.xml", "y", "yarnsiteconfig",
        "Path to a valid yarn-site.xml config file", true),

    // fair-scheduler.xml is not mandatory
    // if FairSchedulerConfiguration.ALLOCATION_FILE is defined in yarn-site.xml
    FAIR_SCHEDULER("fair-scheduler.xml", "f", "fsconfig",
        "Path to a valid fair-scheduler.xml config file", true),
    CONVERSION_RULES("conversion rules config file", "r", "rulesconfig",
        "Optional parameter. If given, should specify a valid path to the " +
            "conversion rules file (property format).", true),
    CONSOLE_MODE("console mode", "p", "print",
        "If defined, the converted configuration will " +
            "only be emitted to the console.", false),
    CLUSTER_RESOURCE("cluster resource", "c", "cluster-resource",
        "Needs to be given if maxResources is defined as percentages " +
            "for any queue, otherwise this parameter can be omitted.",
              true),
    OUTPUT_DIR("output directory", "o", "output-directory",
        "Output directory for yarn-site.xml and" +
            " capacity-scheduler.xml files." +
            "Must have write permission for user who is running this script.",
            true),
    DRY_RUN("dry run", "d", "dry-run", "Performs a dry-run of the conversion." +
            "Outputs whether the conversion is possible or not.", false),
    NO_TERMINAL_RULE_CHECK("no terminal rule check", "t",
        "no-terminal-rule-check",
        "Disables checking whether a placement rule is terminal to maintain" +
        " backward compatibility with configs that were made before YARN-8967.",
        false),
    SKIP_VERIFICATION("skip verification", "s",
        "skip-verification",
        "Skips the verification of the converted configuration", false),
    SKIP_PLACEMENT_RULES_CONVERSION("skip placement rules conversion",
        "sp", "skip-convert-placement-rules",
        "Do not convert placement rules", false),
    ENABLE_ASYNC_SCHEDULER("enable asynchronous scheduler", "a", "enable-async-scheduler",
      "Enables the Asynchronous scheduler which decouples the CapacityScheduler" +
        " scheduling from Node Heartbeats.", false),
    RULES_TO_FILE("rules to external file", "e", "rules-to-file",
        "Generates the converted placement rules to an external JSON file " +
        "called mapping-rules.json", false),
    CONVERT_PERCENTAGES("convert weights to percentages",
        "pc", "percentage",
        "Converts FS queue weights to percentages",
        false),
    DISABLE_PREEMPTION("disable preemption", "dp", "disable-preemption",
        "Disable the preemption with nopolicy or observeonly mode. " +
            "Preemption is enabled by default. " +
            "nopolicy removes ProportionalCapacityPreemptionPolicy from " +
            "the list of monitor policies, " +
            "observeonly sets " +
            "yarn.resourcemanager.monitor.capacity.preemption.observe_only " +
            "to true.", true),
    HELP("help", "h", "help", "Displays the list of options", false);

    private final String name;
    private final String shortSwitch;
    private final String longSwitch;
    private final String description;
    private final boolean hasArg;

    CliOption(String name, String shortSwitch, String longSwitch,
        String description, boolean hasArg) {
      this.name = name;
      this.shortSwitch = shortSwitch;
      this.longSwitch = longSwitch;
      this.description = description;
      this.hasArg = hasArg;
    }

    /**
     * 创建commons-cli对应的Option对象
     * @return commons-cli Option实例
     */
    public Option createCommonsCliOption() {
      Option option = new Option(shortSwitch, longSwitch, hasArg, description);
      return option;
    }
  }

  /**
   * 解析命令行参数并执行配置转换
   * @param args 命令行参数数组
   * @return 转换结果，0成功，-1失败
   * @throws Exception 转换过程中抛出的异常
   */
  int parseAndConvert(String[] args) throws Exception {
    // 创建命令行选项定义
    Options opts = createOptions();
    int retVal = 0;

    try {
      // 无参数时打印帮助信息
      if (args.length == 0) {
        LOG.info("Missing command line arguments");
        printHelp(opts);
        return 0;
      }

      // 解析命令行参数
      CommandLine cliParser = new GnuParser().parse(opts, args);

      // 请求帮助时打印帮助信息
      if (cliParser.hasOption(CliOption.HELP.shortSwitch)) {
        printHelp(opts);
        return 0;
      }

      // 准备参数并获取转换器实例
      FSConfigToCSConfigConverter converter =
          prepareAndGetConverter(cliParser);

      // 执行配置转换
      converter.convert(converterParams);

      // 获取输出目录和跳过验证标记
      String outputDir = converterParams.getOutputDirectory();
      boolean skipVerification =
          cliParser.hasOption(CliOption.SKIP_VERIFICATION.shortSwitch);
      // 非空输出目录且不跳过验证时，验证转换后的配置
      if (outputDir != null && !skipVerification) {
        validator.validateConvertedConfig(
            converterParams.getOutputDirectory());
      }
    } catch (ParseException e) {
      // 命令行解析失败处理
      String msg = "Options parsing failed: " + e.getMessage();
      logAndStdErr(e, msg);
      printHelp(opts);
      retVal = -1;
    } catch (PreconditionException e) {
      // 前置条件检查失败处理
      String msg = "Cannot start FS config conversion due to the following"
          + " precondition error: " + e.getMessage();
      handleException(e, msg);
      retVal = -1;
    } catch (UnsupportedPropertyException e) {
      // 不支持的配置属性处理
      String msg = "Unsupported property/setting encountered during FS config " +
          "conversion: " + e.getMessage();
      handleException(e, msg);
      retVal = -1;
    } catch (ConversionException | IllegalArgumentException e) {
      // 转换过程致命错误处理
      String msg = "Fatal error during FS config conversion: " + e.getMessage();
      handleException(e, msg);
      retVal = -1;
    } catch (VerificationException e) {
      // 配置验证失败处理
      Throwable cause = e.getCause();
      String msg = "Verification failed: " + e.getCause().getMessage();
      conversionOptions.handleVerificationFailure(cause, msg);
      retVal = -1;
    }

    // 解析转换完成后的收尾处理
    conversionOptions.handleParsingFinished();

    return retVal;
  }

  private void handleException(Exception e, String msg) {
    conversionOptions.handleGenericException(e, msg);
  }

  /**
   * 同时输出日志到SLF4J和标准错误流
   * @param t 异常对象
   * @param msg 错误信息
   */
  static void logAndStdErr(Throwable t, String msg) {
    LOG.debug("Stack trace", t);
    LOG.error(msg);
    System.err.println(msg);
  }

  /**
   * 创建所有CLI选项对象
   * @return 完整的CLI选项集合
   */
  private Options createOptions() {
    Options opts = new Options();

    // 遍历枚举创建所有选项
    for (CliOption cliOption : CliOption.values()) {
      opts.addOption(cliOption.createCommonsCliOption());
    }

    return opts;
  }

  /**
   * 解析命令行参数，验证合法性并创建转换器
   * @param cliParser 已解析的命令行参数
   * @return 配置转换器实例
   */
  private FSConfigToCSConfigConverter prepareAndGetConverter(
      CommandLine cliParser) {
    // 获取干运行标记
    boolean dryRun =
        cliParser.hasOption(CliOption.DRY_RUN.shortSwitch);
    conversionOptions.setDryRun(dryRun);
    // 设置是否关闭终止放置规则检查
    conversionOptions.setNoTerminalRuleCheck(
        cliParser.hasOption(CliOption.NO_TERMINAL_RULE_CHECK.shortSwitch));
    // 设置是否启用异步调度器
    conversionOptions.setEnableAsyncScheduler(
      cliParser.hasOption(CliOption.ENABLE_ASYNC_SCHEDULER.shortSwitch));

    // 检查必填参数yarn-site.xml是否存在
    checkOptionPresent(cliParser, CliOption.YARN_SITE);
    // 检查干运行/控制台/输出目录至少有一个被指定
    checkOutputDefined(cliParser, dryRun);

    // 验证输入文件并构建转换参数
    converterParams = validateInputFiles(cliParser);
    // 创建规则处理器
    ruleHandler = new FSConfigToCSConfigRuleHandler(conversionOptions);

    // 获取转换器实例
    return converterFunc.get();
  }

  /**
   * 验证所有输入文件参数，构建转换参数对象
   * @param cliParser 已解析的命令行参数
   * @return 构建完成的转换参数对象
   */
  private FSConfigToCSConfigConverterParams validateInputFiles(
      CommandLine cliParser) {
    // 从命令行获取各参数值
    String yarnSiteXmlFile =
        cliParser.getOptionValue(CliOption.YARN_SITE.shortSwitch);
    String fairSchedulerXmlFile =
        cliParser.getOptionValue(CliOption.FAIR_SCHEDULER.shortSwitch);
    String conversionRulesFile =
        cliParser.getOptionValue(CliOption.CONVERSION_RULES.shortSwitch);
    String outputDir =
        cliParser.getOptionValue(CliOption.OUTPUT_DIR.shortSwitch);
    FSConfigToCSConfigConverterParams.
        PreemptionMode preemptionMode =
        FSConfigToCSConfigConverterParams.
            PreemptionMode.fromString(cliParser.
                getOptionValue(CliOption.DISABLE_PREEMPTION.shortSwitch));

    // 是否转换放置规则
    boolean convertPlacementRules =
        !cliParser.hasOption(
            CliOption.SKIP_PLACEMENT_RULES_CONVERSION.shortSwitch);

    // 验证输入文件合法性
    checkFile(CliOption.YARN_SITE, yarnSiteXmlFile);
    checkFile(CliOption.FAIR_SCHEDULER, fairSchedulerXmlFile);
    checkFile(CliOption.CONVERSION_RULES, conversionRulesFile);
    // 验证输出目录合法性
    checkDirectory(CliOption.OUTPUT_DIR, outputDir);
    // 检查输出目录不包含源文件和目标文件
    checkOutputDirDoesNotContainXmls(yarnSiteXmlFile, outputDir);
    // 验证抢占禁用参数合法性
    if (cliParser.hasOption(CliOption.
        DISABLE_PREEMPTION.shortSwitch)) {
      checkDisablePreemption(preemptionMode);
    }

    // 如果需要输出放置规则到JSON文件，检查输出目录不存在该文件
    if (!cliParser.hasOption(CliOption.CONSOLE_MODE.shortSwitch) &&
        cliParser.hasOption(CliOption.RULES_TO_FILE.shortSwitch)) {
      checkFileNotInOutputDir(new File(outputDir),
          FSConfigToCSConfigConverter.MAPPING_RULES_JSON);
    }

    // 通过Builder构建转换参数对象
    return FSConfigToCSConfigConverterParams.Builder.create()
        .withYarnSiteXmlConfig(yarnSiteXmlFile)
        .withFairSchedulerXmlConfig(fairSchedulerXmlFile)
        .withConversionRulesConfig(conversionRulesFile)
        .withClusterResource(
            cliParser.getOptionValue(CliOption.CLUSTER_RESOURCE.shortSwitch))
        .withConsole(cliParser.hasOption(CliOption.CONSOLE_MODE.shortSwitch))
        .withOutputDirectory(outputDir)
        .withConvertPlacementRules(convertPlacementRules)
        .withPlacementRulesToFile(
            cliParser.hasOption(CliOption.RULES_TO_FILE.shortSwitch))
        .withUsePercentages(
            cliParser.hasOption(CliOption.CONVERT_PERCENTAGES.shortSwitch))
        .withDisablePreemption(preemptionMode)
        .build();
  }

  /**
   * 检查输出目录不包含输入配置和即将生成的配置文件，避免覆盖
   * @param yarnSiteXmlFile 输入yarn-site.xml路径
   * @param outputDir 输出目录路径
   */
  private static void checkOutputDirDoesNotContainXmls(String yarnSiteXmlFile,
      String outputDir) {
    if (yarnSiteXmlFile == null || outputDir == null) {
      return;
    }

    // 检查输入yarn-site.xml的父目录不是输出目录，避免冲突
    File xmlFile = new File(yarnSiteXmlFile);
    File xmlParentFolder = xmlFile.getParentFile();
    File output = new File(outputDir);
    if (output.equals(xmlParentFolder)) {
      throw new IllegalArgumentException(
          String.format(ALREADY_CONTAINS_EXCEPTION_MSG,
              CliOption.OUTPUT_DIR.name, CliOption.OUTPUT_DIR.shortSwitch,
              CliOption.OUTPUT_DIR.longSwitch, CliOption.YARN_SITE.name,
              CliOption.YARN_SITE.shortSwitch,
              CliOption.YARN_SITE.longSwitch));
    }

    // 检查输出目录不存在yarn-site.xml和capacity-scheduler.xml，避免覆盖
    checkFileNotInOutputDir(output,
        YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    checkFileNotInOutputDir(output,
        YarnConfiguration.CS_CONFIGURATION_FILE);
  }

  /**
   * 检查指定文件不存在于输出目录中
   * @param output 输出目录
   * @param fileName 待检查文件名
   */
  private static void checkFileNotInOutputDir(File output, String fileName) {
    File file = new File(output, fileName);
    if (file.exists()) {
      throw new IllegalArgumentException(
          String.format(ALREADY_CONTAINS_FILE_EXCEPTION_MSG,
              CliOption.OUTPUT_DIR.name, output,
              CliOption.OUTPUT_DIR.shortSwitch,
              CliOption.OUTPUT_DIR.longSwitch,
              fileName));
    }
  }

  /**
   * 打印帮助信息到控制台
   * @param opts 命令行选项定义