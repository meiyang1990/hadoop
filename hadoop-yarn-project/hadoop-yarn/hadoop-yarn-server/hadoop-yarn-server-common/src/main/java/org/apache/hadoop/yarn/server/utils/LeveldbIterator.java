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

package org.apache.hadoop.yarn.server.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

/**
 * LevelDB迭代器包装类，用于将原生LevelDB抛出的运行时异常统一转换为DBException。
 * 封装了LevelDB DBIterator，统一异常处理，方便上层代码使用。
 */
@Public
@Evolving
public class LeveldbIterator implements Iterator<Map.Entry<byte[], byte[]>>,
                                        Closeable {
  private DBIterator iter;

  /**
   * 基于指定数据库创建迭代器。
   *
   * @param db LevelDB数据库实例
   */
  public LeveldbIterator(DB db) {
    iter = db.iterator();
  }

  /**
   * 基于指定数据库和读选项创建迭代器。
   *
   * @param db LevelDB数据库实例
   * @param options LevelDB读选项
   */
  public LeveldbIterator(DB db, ReadOptions options) {
    iter = db.iterator(options);
  }

  /**
   * 直接使用底层DBIterator构造包装迭代器。
   *
   * @param iter 底层LevelDB迭代器实例
   */
  public LeveldbIterator(DBIterator iter) {
    this.iter = iter;
  }

  /**
   * 将迭代器定位到第一个键大于等于目标键的位置。
   *
   * @param key 目标查找键
   * @throws DBException 转换后的LevelDB异常
   */
  public void seek(byte[] key) throws DBException {
    try {
      iter.seek(key);
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 将迭代器定位到数据库起始位置。
   * @throws DBException 转换后的LevelDB异常
   */
  public void seekToFirst() throws DBException {
    try {
      iter.seekToFirst();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 将迭代器定位到数据库末尾位置。
   * @throws DBException 转换后的LevelDB异常
   */
  public void seekToLast() throws DBException {
    try {
      iter.seekToLast();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 检查迭代是否还有更多元素。
   * @return true 存在下一个元素，false 遍历完成
   * @throws DBException 转换后的LevelDB异常
   */
  public boolean hasNext() throws DBException {
    try {
      return iter.hasNext();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 获取迭代中下一个元素并前进迭代器。
   *
   * @return 键值对条目
   * @throws DBException 转换后的LevelDB异常
   */
  @Override
  public Map.Entry<byte[], byte[]> next() throws DBException {
    try {
      return iter.next();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 获取下一个元素，不前进迭代器。
   *
   * @return 下一个键值对条目
   * @throws DBException 转换后的LevelDB异常
   */
  public Map.Entry<byte[], byte[]> peekNext() throws DBException {
    try {
      return iter.peekNext();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 检查迭代是否还有前一个元素。
   * @return true 存在前一个元素，false 已到起始位置
   * @throws DBException 转换后的LevelDB异常
   */
  public boolean hasPrev() throws DBException {
    try {
      return iter.hasPrev();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 获取前一个元素并回退迭代器。
   * @return 前一个键值对条目
   * @throws DBException 转换后的LevelDB异常
   */
  public Map.Entry<byte[], byte[]> prev() throws DBException {
    try {
      return iter.prev();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 获取前一个元素，不回退迭代器。
   * @return 前一个键值对条目
   * @throws DBException 转换后的LevelDB异常
   */
  public Map.Entry<byte[], byte[]> peekPrev() throws DBException {
    try {
      return iter.peekPrev();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 从数据库中删除迭代器当前返回的元素。
   * @throws DBException 转换后的LevelDB异常
   */
  @Override
  public void remove() throws DBException {
    try {
      iter.remove();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * 关闭迭代器，释放底层资源。
   * @throws IOException 关闭时发生IO异常
   */
  @Override
  public void close() throws IOException {
    try {
      iter.close();
    } catch (RuntimeException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}