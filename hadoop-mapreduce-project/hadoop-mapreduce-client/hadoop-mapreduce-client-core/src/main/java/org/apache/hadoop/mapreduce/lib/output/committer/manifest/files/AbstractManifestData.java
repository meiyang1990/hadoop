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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.JsonSerialization;

import static java.util.Objects.requireNonNull;

/**
 * 文件提交清单数据的抽象基类，为单次/多次作业提交的数据结构提供公共能力，
 * 提供路径序列化、数据验证和JSON序列化等通用能力。
 */
@SuppressWarnings("serial")
@InterfaceAudience.Private
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractManifestData<T extends AbstractManifestData>
    implements Serializable, IOStatisticsSource {


  /**
   * 将Path对象序列化为可存入JSON的字符串形式。
   * @param path 待序列化的路径
   * @return 序列化后的路径字符串，输入为null时返回null
   */
  public static String marshallPath(@Nullable Path path) {
    return path != null
        ? path.toUri().toString()
        : null;
  }

  /**
   * 将JSON中的路径字符串反序列化为Path对象，通过URI转换实现。
   * @param path 字符串形式的路径
   * @return 反序列化后的Path对象
   * @throws RuntimeException 反序列化失败时抛出
   */
  public static Path unmarshallPath(String path) {
    try {
      return new Path(new URI(requireNonNull(path, "No path")));
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "Failed to parse \"" + path + "\" : " + e,
          e);
    }
  }

  /**
   * 验证清单数据完整性，检查必填字段是否已正确设置。
   * @return 验证通过后的实例本身
   * @throws IOException 数据验证不通过时抛出
   */
  public abstract T validate() throws IOException;

  /**
   * 先验证数据，然后将清单数据序列化为JSON格式的字节数组。
   * @return 序列化后的字节数组，可用于持久化存储
   * @throws IOException 验证失败或序列化错误时抛出
   */
  public abstract byte[] toBytes() throws IOException;

  /**
   * 将清单数据保存到Hadoop文件系统的指定路径。
   * @param fs 目标文件系统
   * @param path 保存路径
   * @param overwrite 是否覆盖已存在文件
   * @throws IOException IO操作异常或验证失败时抛出
   */
  public abstract void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException;

  /**
   * 获取当前类型对应的JSON序列化器实例。
   * @return 当前清单数据类型对应的JSON序列化器
   */
  public abstract JsonSerialization<T> createSerializer();

  /**
   * 验证集合中所有元素都属于指定类型。
   * @param it 集合迭代器
   * @param classname 要求的元素类型
   * @throws IOException 存在元素类型不匹配时抛出
   */
  void validateCollectionClass(Iterable it, Class classname)
      throws IOException {
    for (Object o : it) {
      verify(o.getClass().equals(classname),
          "Collection element is not a %s: %s", classname, o.getClass());
    }
  }

  /**
   * 验证条件是否成立，不成立则抛出IO异常。
   * @param expression 必须为true的验证条件
   * @param message 验证失败时的错误消息
   * @param args 错误消息格式化参数
   * @throws IOException 条件不成立时抛出
   */

  static void verify(boolean expression,
      String message,
      Object... args) throws IOException {
    if (!expression) {
      throw new IOException(String.format(message, args));
    }
  }
}