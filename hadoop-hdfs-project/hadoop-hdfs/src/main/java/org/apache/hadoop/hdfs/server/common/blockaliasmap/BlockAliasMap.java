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
package org.apache.hadoop.hdfs.server.common.blockaliasmap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.BlockAlias;

/**
 * HDFS提供块（Provided Blocks）的别名映射抽象基类，用于管理外部存储提供的块与HDFS原生块的映射关系。
 * 定义了读写别名映射的统一抽象接口，支持不同存储介质的具体实现。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class BlockAliasMap<T extends BlockAlias> {

  /**
   * 不支持remove操作的不可变迭代器，用于遍历提供块的别名列表。
   * 遵循提供存储的只读特性，禁止修改迭代过程中的集合。
   */
  public abstract class ImmutableIterator implements Iterator<T> {
    public void remove() {
      throw new UnsupportedOperationException(
          "Remove is not supported for provided storage");
    }
  }

  /**
   * 提供块别名映射的读取器抽象基类，定义了查询和遍历别名映射的接口。
   */
  public static abstract class Reader<U extends BlockAlias>
      implements Iterable<U>, Closeable {

    /**
     * 别名映射读取器配置选项的标记接口，用于扩展不同实现的配置参数。
     */
    public interface Options { }

    /**
     * 根据HDFS块标识解析对应的块别名信息。
     * @param ident 需要解析的HDFS块对象
     * @return 解析得到的块别名，如果不存在则返回空Optional
     * @throws IOException 解析过程中发生IO异常时抛出
     */
    public abstract Optional<U> resolve(Block ident) throws IOException;
  }

  /**
   * 获取指定块池的别名映射读取器。
   * @param opts 读取器配置选项
   * @param blockPoolID 目标块池ID
   * @return 指定块池的别名映射读取器，如果无法创建则返回null
   * @throws IOException 获取读取器过程中发生IO异常时抛出
   */
  public abstract Reader<T> getReader(Reader.Options opts, String blockPoolID)
      throws IOException;

  /**
   * 提供块别名映射的写入器抽象基类，定义了存储别名映射的接口。
   */
  public static abstract class Writer<U extends BlockAlias>
      implements Closeable {
    /**
     * 别名映射写入器配置选项的标记接口，用于扩展不同实现的配置参数。
     */
    public interface Options { }

    /**
     * 将一个块别名信息存储到别名映射中。
     * @param token 需要存储的块别名对象
     * @throws IOException 存储过程中发生IO异常时抛出
     */
    public abstract void store(U token) throws IOException;

  }

  /**
   * 获取指定块池的别名映射写入器。
   * @param opts 写入器配置选项
   * @param blockPoolID 目标块池ID
   * @return 指定块池的别名映射写入器
   * @throws IOException 获取写入器过程中发生IO异常时抛出
   */
  public abstract Writer<T> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException;

  /**
   * 刷新别名映射，重新加载最新的映射数据。
   * @throws IOException 刷新过程中发生IO异常时抛出
   */
  public abstract void refresh() throws IOException;

  /**
   * 关闭别名映射，释放占用的资源。
   * @throws IOException 关闭过程中发生IO异常时抛出
   */
  public abstract void close() throws IOException;

}