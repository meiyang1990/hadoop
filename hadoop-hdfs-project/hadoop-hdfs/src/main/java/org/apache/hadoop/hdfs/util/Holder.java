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
package org.apache.hadoop.hdfs.util;

/**
 * 泛型对象持有器，对任意类型对象进行包装。
 * 主要用于在集合中存储不可变对象（如包装类型Integer）时，
 * 避免重复查询查找的性能开销，简化可变引用场景开发。
 * 
 * @param <T> 被持有对象的类型
 */
public class Holder<T> {
  public T held;
  
  /**
   * 构造方法，创建持有指定对象的Holder实例
   * @param held 被持有的对象
   */
  public Holder(T held) {
    this.held = held;
  }
  
  @Override
  public String toString() {
    return String.valueOf(held);
  }
}