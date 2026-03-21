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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

import org.jsonschema2pojo.DefaultGenerationConfig;
import org.jsonschema2pojo.GenerationConfig;
import org.jsonschema2pojo.Jackson2Annotator;
import org.jsonschema2pojo.SchemaGenerator;
import org.jsonschema2pojo.SchemaMapper;
import org.jsonschema2pojo.SchemaStore;
import org.jsonschema2pojo.rules.RuleFactory;

import com.sun.codemodel.JCodeModel;

/**
 * 文件级：根据队列放置规则JSON schema自动生成Java POJO类的工具类，用于容量调度器放置规则配置解析
 * Helper class to re-generate java POJOs based on the JSON schema.
 */
public final class GeneratePojos {
  @SuppressWarnings("checkstyle:linelength")
  // 生成的POJO类目标包名
  private static final String TARGET_PACKAGE =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema";

  private GeneratePojos() {
    // 工具类禁止实例化
  }

  /**
   * 工具入口方法，根据JSON schema生成POJO类并输出到源码目录
   * @param args 命令行参数（未使用）
   * @throws IOException 读写schema文件或输出源码时抛出IO异常
   */
  public static void main(String[] args) throws IOException {
    // 创建代码模型对象，存储生成的Java代码结构
    JCodeModel codeModel = new JCodeModel();
    // 加载输入的JSON schema定义文件
    URL schemaURL = Paths.get(
        "src/main/json_schema/MappingRulesDescription.json").toUri().toURL();

    // 配置jsonschema2pojo代码生成参数
    GenerationConfig config = new DefaultGenerationConfig() {
      @Override
      // 不生成Builder模式代码
      public boolean isGenerateBuilders() {
        return false;
      }

      @Override
      // 使用原生类型代替包装类型
      public boolean isUsePrimitives() {
          return true;
      }
    };

    // 创建schema映射器，配置代码生成工厂和schema生成器
    SchemaMapper mapper =
        new SchemaMapper(
            new RuleFactory(config,
                new Jackson2Annotator(config),
                new SchemaStore()),
            new SchemaGenerator());

    // 根据JSON schema生成代码结构到codeModel
    mapper.generate(codeModel, "ignore", TARGET_PACKAGE, schemaURL);

    // 将生成的POJO源码输出到指定目录
    codeModel.build(new File("src/main/java"));
  }
}