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

package org.apache.hadoop.mapreduce.jobhistory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/**
 * Avro数组与Java基本类型数组之间的转换工具类
 * 用于MapReduce作业历史日志序列化时，处理进度切分点整数数组的Avro格式转换
 */
public class AvroArrayUtils {

  /** 预定义int类型数组的Avro schema */
  private static final Schema ARRAY_INT
      = Schema.createArray(Schema.create(Schema.Type.INT));

  /** 空进度切分数组常量，用于表示无切分点的场景 */
  static public List<Integer> NULL_PROGRESS_SPLITS_ARRAY
    = new GenericData.Array<Integer>(0, ARRAY_INT);

  /**
   * 将Java基本类型int数组转换为Avro兼容的List<Integer>格式
   * @param values 原始Java int数组
   * @return Avro兼容的List<Integer>，可直接用于Avro序列化
   */
  public static List<Integer>
    toAvro(int values[]) {
    List<Integer> result = new ArrayList<Integer>(values.length);

    for (int i = 0; i < values.length; ++i) {
      result.add(values[i]);
    }

    return result;
  }

  /**
   * 将Avro序列化后的List<Integer>转换回Java基本类型int数组
   * @param avro Avro反序列化得到的List<Integer>
   * @return 转换后的Java基本类型int数组
   */
  public static int[] fromAvro(List<Integer> avro) {
    int[] result = new int[avro.size()];

    int i = 0;
      
    for (Iterator<Integer> iter = avro.iterator(); iter.hasNext(); ++i) {
      result[i] = iter.next();
    }

    return result;
  }
}