// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件输出格式使用的PathOutputCommitter工厂基类，用于创建不同存储系统对应的输出提交器。
 * 选择逻辑：
 * <ol>
 *   <li>如果配置中指定了明确的提交器工厂类，则使用指定工厂</li>
 *   <li>如果未指定，但输出路径非空且对应文件系统 schema 配置了专属工厂，则使用该schema对应的工厂</li>
 *   <li>否则，默认创建FileOutputCommitter实例</li>
 * </ol>
 * 供FileOutputFormat调用，为每个任务尝试创建对应的输出提交器。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PathOutputCommitterFactory extends Configured {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathOutputCommitterFactory.class);

  /**
   * 全局输出提交器工厂配置项名称，当没有schema专属配置时使用该配置指定的工厂。
   */
  public static final String COMMITTER_FACTORY_CLASS =
      "mapreduce.outputcommitter.factory.class";

  /**
   * 按文件系统schema配置提交器工厂的配置前缀。
   */
  public static final String COMMITTER_FACTORY_SCHEME =
      "mapreduce.outputcommitter.factory.scheme";

  /**
   * 按文件系统schema配置提交器工厂的配置名格式。
   */
  public static final String COMMITTER_FACTORY_SCHEME_PATTERN =
      COMMITTER_FACTORY_SCHEME + ".%s";


  /**
   * 默认文件系统提交器工厂类全限定名。
   */
  public static final String FILE_COMMITTER_FACTORY  =
      "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory";

  /**
   * 命名提交器工厂类全限定名，通过类名创建指定提交器。
   */
  public static final String NAMED_COMMITTER_FACTORY  =
      "org.apache.hadoop.mapreduce.lib.output.NamedCommitterFactory";

  /**
   * 命名提交器的目标提交器类名配置项。
   */
  public static final String NAMED_COMMITTER_CLASS =
      "mapreduce.outputcommitter.named.classname";

  /**
   * 默认提交器工厂类名常量。
   */
  public static final String COMMITTER_FACTORY_DEFAULT =
      FILE_COMMITTER_FACTORY;

  /**
   * 为指定任务尝试创建输出提交器。
   * @param outputPath 输出路径，可为null
   * @param context 任务尝试上下文
   * @return 新的输出提交器实例
   * @throws IOException 创建提交器过程中出现IO异常
   */
  public PathOutputCommitter createOutputCommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    return createFileOutputCommitter(outputPath, context);
  }

  /**
   * 创建默认的FileOutputCommitter实例，供基类使用。
   * @param outputPath 任务输出路径，未定义时可为null
   * @param context 任务尝试上下文
   * @return 要使用的输出提交器
   * @throws IOException 创建提交器过程中出现IO异常
   */
  protected final PathOutputCommitter createFileOutputCommitter(
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    LOG.debug("Creating FileOutputCommitter for path {} and context {}",
        outputPath, context);
    return new FileOutputCommitter(outputPath, context);
  }

  /**
   * 根据配置和输出路径获取对应输出提交器工厂实例。
   * @param outputPath 作业输出路径，为null时无法确定schema，使用默认工厂
   * @param conf 作业配置
   * @return 初始化完成的提交器工厂实例
   */
  public static PathOutputCommitterFactory getCommitterFactory(
      Path outputPath,
      Configuration conf) {
    // 决定使用全局配置还是schema专属配置
    LOG.debug("Looking for committer factory for path {}", outputPath);
    String key = COMMITTER_FACTORY_CLASS;
    if (StringUtils.isEmpty(conf.getTrimmed(key)) && outputPath != null) {
      // 没有全局指定工厂，且存在输出路径，获取输出路径的schema
      String scheme = outputPath.toUri().getScheme();

      // 检查该schema是否配置了专属工厂
      String schemeKey = String.format(COMMITTER_FACTORY_SCHEME_PATTERN,
          scheme);
      if (StringUtils.isNotEmpty(conf.getTrimmed(schemeKey))) {
        // 存在schema专属配置，使用该配置查找工厂类
        LOG.info("Using schema-specific factory for {}", outputPath);
        key = schemeKey;
      } else {
        LOG.debug("No scheme-specific factory defined in {}", schemeKey);
      }
    }

    // 创建工厂实例，先检查配置项是否为空，避免Configuration.getClass抛出异常
    Class<? extends PathOutputCommitterFactory> factory;
    String trimmedValue = conf.getTrimmed(key, "");
    if (StringUtils.isEmpty(trimmedValue)) {
      // 配置为空，使用默认工厂
      LOG.info("No output committer factory defined,"
          + " defaulting to FileOutputCommitterFactory");
      factory = FileOutputCommitterFactory.class;
    } else {
      // 配置已设置，加载对应工厂类
      factory = conf.getClass(key,
          FileOutputCommitterFactory.class,
          PathOutputCommitterFactory.class);
      LOG.info("Using OutputCommitter factory class {} from key {}",
          factory, key);
    }
    // 通过反射实例化工厂并注入配置
    return ReflectionUtils.newInstance(factory, conf);
  }

  /**
   * 工具方法：获取对应工厂后直接创建输出提交器。
   * @param outputPath 任务输出路径，未定义时可为null
   * @param context 任务尝试上下文
   * @return 要使用的输出提交器
   * @throws IOException 创建提交器过程中出现IO异常
   */
  public static PathOutputCommitter createCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    return getCommitterFactory(outputPath,
        context.getConfiguration())
        .createOutputCommitter(outputPath, context);
  }

}