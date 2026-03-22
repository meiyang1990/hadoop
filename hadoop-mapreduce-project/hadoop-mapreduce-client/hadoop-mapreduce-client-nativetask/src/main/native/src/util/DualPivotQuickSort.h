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

/**
 * @file DualPivotQuickSort.h
 * @brief 双枢轴快速排序实现头文件，为NativeTask提供高效排序能力
 */

#ifndef DUALPIVOTQUICKSORT_H_
#define DUALPIVOTQUICKSORT_H_

#include <stdint.h>
#include <algorithm>

namespace NativeTask {

/**
 * @brief 双枢轴快速排序模板函数，对指定范围的uint32_t数组排序
 * @param elements 待排序向量引用
 * @param left 排序范围左边界索引
 * @param right 排序范围右边界索引
 * @param div 分割系数，用于计算两个枢轴的位置
 * @param compare 自定义比较函数对象，用于元素比较
 */
template<typename _Compare>
void DualPivotQuicksort(std::vector<uint32_t> & elements, int left, int right, int div,
    _Compare compare) {

  if (left >= right) {
    return;
  }

  // 获取向量底层数组首地址
  uint32_t * e = &(elements[0]);
  int len = right - left;

  if (len < 27) { // 小数组使用插入排序，提升性能
    for (int i = left + 1; i <= right; i++) {
      for (int j = i; j > left && compare(e[j - 1], e[j]) > 0; --j) {
        std::swap(e[j], e[j - 1]);
      }
    }
    return;
  }

  // 计算步长，用于选取两个中值位置
  int third = len / div;

  // 计算两个候选中值位置
  int m1 = left + third;
  int m2 = right - third;

  // 边界校正，确保不越界
  if (m1 <= left) {
    m1 = left + 1;
  }
  if (m2 >= right) {
    m2 = right - 1;
  }
  // 排序两个候选枢轴，将较小枢轴放到左边界，较大枢轴放到右边界
  if (compare(e[m1], e[m2]) < 0) {
    std::swap(e[m1], e[left]);
    std::swap(e[m2], e[right]);
  } else {
    std::swap(e[m1], e[right]);
    std::swap(e[m2], e[left]);
  }

  // 记录两个枢轴的位置
  int pivot1 = left;
  int pivot2 = right;

  // 分区指针：less指向小于左枢轴区域的下一个位置，great指向大于右枢轴区域的前一个位置
  int less = left + 1;
  int great = right - 1;

  // 遍历中间区域进行分区
  for (int k = less; k <= great; k++) {
    // 当前元素小于左枢轴，交换到less区域
    if (compare(e[k], e[pivot1]) < 0) {
      std::swap(e[k], e[less]);
      less++;
    } else if (compare(e[k], e[pivot2]) > 0) {
      // 当前元素大于右枢轴，从右向左找第一个不大于右枢轴的元素交换
      while (k < great && compare(e[great], e[pivot2]) > 0) {
        --great;
      }
      std::swap(e[k], e[great]);
      great--;

      // 交换后检查新元素是否小于左枢轴，需要处理
      if (compare(e[k], e[pivot1]) < 0) {
        std::swap(e[k], e[less]);
        less++;
      }
    }
  }
  // 将枢轴放到正确的分区位置
  int dist = great - less;

  // 分区太小，增大分割系数，提升后续分区均衡性
  if (dist < 13) {
    ++div;
  }
  std::swap(e[less - 1], e[left]);
  std::swap(e[great + 1], e[right]);

  // 递归排序左右两个分区（小于左枢轴 和 大于右枢轴的区域）
  DualPivotQuicksort(elements, left, less - 2, div, compare);
  DualPivotQuicksort(elements, great + 2, right, div, compare);

  // 处理中间区域元素与枢轴相等的情况，优化重复元素排序性能
  if (dist > len - 13 && pivot1 != pivot2) {
    for (int k = less; k <= great; ++k) {
      if (0 == compare(e[k], e[pivot1])) {
        std::swap(e[k], e[less]);
        less++;
      } else if (0 == compare(e[k], e[pivot2])) {
        std::swap(e[k], e[great]);
        great--;
        // 交换后再次检查是否等于左枢轴
        if (0 == compare(e[k], e[pivot1])) {
          std::swap(e[k], e[less]);
          less++;
        }
      }
    }
  }

  // 递归排序中间区域（介于两个枢轴之间的元素）
  if (compare(e[pivot1], e[pivot2]) < 0) {
    DualPivotQuicksort(elements, less, great, div, compare);
  }
}

/**
 * @brief 双枢轴快速排序入口函数，对整个向量排序
 * @param elements 待排序向量引用
 * @param compare 自定义比较函数对象，用于元素比较
 */
template<typename _Compare>
void DualPivotQuicksort(std::vector<uint32_t> & elements, _Compare compare) {
  DualPivotQuicksort(elements, 0, elements.size() - 1, 3, compare);
}

} // namespace NativeTask

#endif /* DUALPIVOTQUICKSORT_H_ */