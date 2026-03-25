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

/**
 * 公平调度器(FS)转容量调度器(CS)配置转换工具的参数承载POJO，存储所有转换所需的配置参数。
 *
 */
public final class FSConfigToCSConfigConverterParams {
  private String yarnSiteXmlConfig;
  private String fairSchedulerXmlConfig;
  private String conversionRulesConfig;
  private boolean console;
  private String clusterResource;
  private String outputDirectory;
  private boolean convertPlacementRules;
  private boolean placementRulesToFile;
  private boolean usePercentages;
  private PreemptionMode preemptionMode;

  /**
   * 抢占模式枚举，定义转换后容量调度器的抢占配置模式。
   */
  public enum PreemptionMode {
    ENABLED("enabled"),
    NO_POLICY("nopolicy"),
    OBSERVE_ONLY("observeonly");

    private String cliOption;

    PreemptionMode(String cliOption) {
      this.cliOption = cliOption;
    }

    /**
     * 获取对应命令行参数选项值。
     * @return 命令行参数值字符串
     */
    public String getCliOption() {
      return cliOption;
    }

    /**
     * 根据命令行输入字符串解析获取抢占模式枚举。
     * @param cliOption 命令行输入的模式字符串
     * @return 解析后的抢占模式枚举，默认返回ENABLED
     */
    public static PreemptionMode fromString(String cliOption) {
      if (cliOption != null && cliOption.trim().
          equals(PreemptionMode.OBSERVE_ONLY.getCliOption())) {
        return PreemptionMode.OBSERVE_ONLY;
      } else if (cliOption != null && cliOption.trim().
          equals(PreemptionMode.NO_POLICY.getCliOption())) {
        return PreemptionMode.NO_POLICY;
      } else {
        return PreemptionMode.ENABLED;
      }
    }
  }

  private FSConfigToCSConfigConverterParams() {
    //must use builder
  }

  public String getFairSchedulerXmlConfig() {
    return fairSchedulerXmlConfig;
  }

  public String getYarnSiteXmlConfig() {
    return yarnSiteXmlConfig;
  }

  public String getConversionRulesConfig() {
    return conversionRulesConfig;
  }

  public String getClusterResource() {
    return clusterResource;
  }

  public boolean isConsole() {
    return console;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public boolean isConvertPlacementRules() {
    return convertPlacementRules;
  }

  public boolean isPlacementRulesToFile() {
    return placementRulesToFile;
  }

  public boolean isUsePercentages() {
    return usePercentages;
  }

  public PreemptionMode getPreemptionMode() {
    return preemptionMode;
  }

  @Override
  public String toString() {
    return "FSConfigToCSConfigConverterParams{" +
        "yarnSiteXmlConfig='" + yarnSiteXmlConfig + '\'' +
        ", fairSchedulerXmlConfig='" + fairSchedulerXmlConfig + '\'' +
        ", conversionRulesConfig='" + conversionRulesConfig + '\'' +
        ", clusterResource='" + clusterResource + '\'' +
        ", console=" + console + '\'' +
        ", convertPlacementRules=" + convertPlacementRules +
        ", placementRulesToFile=" + placementRulesToFile +
        '}';
  }

  /**
   * FSConfigToCSConfigConverterParams 对象的构建器，使用Builder模式构造参数对象。
   *
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String yarnSiteXmlConfig;
    private String fairSchedulerXmlConfig;
    private String conversionRulesConfig;
    private boolean console;
    private String clusterResource;
    private String outputDirectory;
    private boolean convertPlacementRules;
    private boolean placementRulesToFile;
    private boolean usePercentages;
    private PreemptionMode preemptionMode;

    private Builder() {
    }

    /**
     * 创建构建器实例。
     * @return 新的构建器实例
     */
    public static Builder create() {
      return new Builder();
    }

    /**
     * 设置yarn-site.xml配置文件路径。
     * @param config 配置文件路径
     * @return 构建器自身
     */
    public Builder withYarnSiteXmlConfig(String config) {
      this.yarnSiteXmlConfig = config;
      return this;
    }

    /**
     * 设置公平调度器分配配置文件路径。
     * @param config 配置文件路径
     * @return 构建器自身
     */
    public Builder withFairSchedulerXmlConfig(String config) {
      this.fairSchedulerXmlConfig = config;
      return this;
    }

    /**
     * 设置转换规则配置文件路径。
     * @param config 配置文件路径
     * @return 构建器自身
     */
    public Builder withConversionRulesConfig(String config) {
      this.conversionRulesConfig = config;
      return this;
    }

    /**
     * 设置集群总资源定义。
     * @param res 集群总资源字符串
     * @return 构建器自身
     */
    public Builder withClusterResource(String res) {
      this.clusterResource = res;
      return this;
    }

    /**
     * 设置是否输出到控制台。
     * @param console 是否输出到控制台
     * @return 构建器自身
     */
    public Builder withConsole(boolean console) {
      this.console = console;
      return this;
    }

    /**
     * 设置转换结果输出目录。
     * @param outputDir 输出目录路径
     * @return 构建器自身
     */
    public Builder withOutputDirectory(String outputDir) {
      this.outputDirectory = outputDir;
      return this;
    }

    /**
     * 设置是否转换放置规则。
     * @param convertPlacementRules 是否转换放置规则
     * @return 构建器自身
     */
    public Builder withConvertPlacementRules(boolean convertPlacementRules) {
      this.convertPlacementRules = convertPlacementRules;
      return this;
    }

    /**
     * 设置是否将放置规则单独输出到文件。
     * @param rulesToFile 是否输出到单独文件
     * @return 构建器自身
     */
    public Builder withPlacementRulesToFile(boolean rulesToFile) {
      this.placementRulesToFile = rulesToFile;
      return this;
    }

    /**
     * 设置是否使用百分比方式定义队列容量。
     * @param usePercentages 是否使用百分比
     * @return 构建器自身
     */
    public Builder withUsePercentages(boolean usePercentages) {
      this.usePercentages = usePercentages;
      return this;
    }

    /**
     * 设置抢占模式。
     * @param preemptionMode 抢占模式枚举
     * @return 构建器自身
     */
    public Builder withDisablePreemption(PreemptionMode preemptionMode) {
      this.preemptionMode = preemptionMode;
      return this;
    }

    /**
     * 构造最终的参数对象。
     * @return 填充完成的转换参数对象
     */
    public FSConfigToCSConfigConverterParams build() {
      FSConfigToCSConfigConverterParams params =
          new FSConfigToCSConfigConverterParams();
      params.clusterResource = this.clusterResource;
      params.console = this.console;
      params.fairSchedulerXmlConfig = this.fairSchedulerXmlConfig;
      params.yarnSiteXmlConfig = this.yarnSiteXmlConfig;
      params.conversionRulesConfig = this.conversionRulesConfig;
      params.outputDirectory = this.outputDirectory;
      params.convertPlacementRules = this.convertPlacementRules;
      params.placementRulesToFile = this.placementRulesToFile;
      params.usePercentages = this.usePercentages;
      params.preemptionMode = this.preemptionMode;
      return params;
    }
  }
}