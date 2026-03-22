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
package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件说明：基于ArrayList实现的可重置迭代器，用于MapReduce连接操作中存储和重放数据元素
 * 
 * 该类实现了ResetableIterator接口，使用ArrayList存储添加的元素，支持按请求重放已有元素。
 * 在性能要求更高的场景下推荐使用{@link StreamBackedIterator}。
 * 主要用于MapReduce端连接操作中，对多个输入源的数据进行缓存和重复遍历。
 * @param <X> 存储元素类型，必须实现Writable接口
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayListBackedIterator<X extends Writable>
    implements ResetableIterator<X> {

  private Iterator<X> iter;
  private ArrayList<X> data;
  private X hold = null;
  private Configuration conf = new Configuration();

  /**
   * 构造空的基于ArrayList的可重置迭代器
   */
  public ArrayListBackedIterator() {
    this(new ArrayList<X>());
  }

  /**
   * 使用已有的ArrayList数据构造可重置迭代器
   * @param data 用于存储迭代元素的ArrayList
   */
  public ArrayListBackedIterator(ArrayList<X> data) {
    this.data = data;
    this.iter = this.data.iterator();
  }

  /**
   * 检查迭代器是否还有下一个元素
   * @return 还有元素返回true，否则返回false
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * 获取下一个元素并复制到给定对象中，同时缓存当前元素用于后续重放
   * @param val 接收下一个元素数据的对象
   * @return 成功获取元素返回true，无更多元素返回false
   * @throws IOException 序列化复制过程可能抛出IO异常
   */
  public boolean next(X val) throws IOException {
    if (iter.hasNext()) {
      ReflectionUtils.copy(conf, iter.next(), val);
      if (null == hold) {
        // 首次获取元素，创建缓存副本
        hold = WritableUtils.clone(val, null);
      } else {
        // 更新缓存为当前元素副本
        ReflectionUtils.copy(conf, val, hold);
      }
      return true;
    }
    return false;
  }

  /**
   * 重放上一次获取的元素，将缓存的数据复制到给定对象中
   * @param val 接收重放元素数据的对象
   * @return 始终返回true表示重放成功
   * @throws IOException 序列化复制过程可能抛出IO异常
   */
  public boolean replay(X val) throws IOException {
    ReflectionUtils.copy(conf, hold, val);
    return true;
  }

  /**
   * 重置迭代器，将迭代位置恢复到集合开头，重新开始遍历
   */
  public void reset() {
    iter = data.iterator();
  }

  /**
   * 向迭代器集合添加新元素，存储元素的深度副本
   * @param item 要添加的元素
   * @throws IOException 克隆过程可能抛出IO异常
   */
  public void add(X item) throws IOException {
    data.add(WritableUtils.clone(item, null));
  }

  /**
   * 关闭迭代器，释放引用帮助GC回收内存
   * @throws IOException 关闭过程可能抛出IO异常
   */
  public void close() throws IOException {
    iter = null;
    data = null;
  }

  /**
   * 清空迭代器中所有元素，重置迭代位置到开头
   */
  public void clear() {
    data.clear();
    reset();
  }
}