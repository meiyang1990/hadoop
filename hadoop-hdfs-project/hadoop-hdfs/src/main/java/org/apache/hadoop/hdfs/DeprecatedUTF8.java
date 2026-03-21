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

package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * HDFS包私有工具类，对已废弃的{@link org.apache.hadoop.io.UTF8}做封装
 * <p>
 * 仅在必须使用旧版UTF8类型时使用本类，通过本类统一抑制编译警告，
 * 避免业务代码各处都添加@SuppressWarnings注解。类名本身隐含了
 * 该类型已废弃的语义。
 * <p>
 * 设计目的：兼容遗留代码，避免分散的废弃警告，保持代码整洁。
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class DeprecatedUTF8 extends org.apache.hadoop.io.UTF8 {
  
  /** 空构造函数 */
  public DeprecatedUTF8() {
    super();
  }

  /** 从字符串构造实例 */
  public DeprecatedUTF8(String string) {
    super(string);
  }

  /** 从另一个DeprecatedUTF8实例拷贝构造 */
  public DeprecatedUTF8(DeprecatedUTF8 utf8) {
    super(utf8);
  }
  
  /* 封装最常用的两个静态方法，避免编辑器对调用处提示废弃警告 */
  
  /**
   * 从DataInput中读取UTF8编码字符串
   * @param in 输入流
   * @return 解码后的字符串
   * @throws IOException 读取或解码失败时抛出
   */
  public static String readString(DataInput in) throws IOException {
    return org.apache.hadoop.io.UTF8.readString(in);
  }
  
  /**
   * 将字符串按UTF8编码写入DataOutput
   * @param out 输出流
   * @param s 待写入字符串
   * @return 写入的字节数
   * @throws IOException 写入或编码失败时抛出
   */
  public static int writeString(DataOutput out, String s) throws IOException {
    return org.apache.hadoop.io.UTF8.writeString(out, s);
  }
}