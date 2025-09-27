//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width_ <= 0 || depth_ <= 0) {
    throw std::invalid_argument("Invalid argument");
  }

  data_.resize(depth_, std::vector<size_t>(width_, 0));
  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  width_ = other.width_;
  depth_ = other.depth_;
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(other.hash_functions_[i]);
  }
  data_ = other.data_;
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  width_ = other.width_;
  depth_ = other.depth_;
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(other.hash_functions_[i]);
  }
  data_ = other.data_;
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  if (width_ <= 0 || depth_ <= 0) {
    std::cout << "Fuck god!!!" << std::endl;
  }
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    size_t result = hash_functions_[i](item) % width_;
    locks_[i % 100].lock();
    data_[i][result] += 1;
    locks_[i % 100].unlock();
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ <= 0 || depth_ <= 0) {
    std::cout << "Fuck god!!!" << std::endl;
  }
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }

  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      data_[i][j] += other.data_[i][j];
    }
  }
  /** @TODO(student) Implement this function! */
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  if (width_ <= 0 || depth_ <= 0) {
    std::cout << "Fuck god!!!" << std::endl;
    return 0;
  }
  uint32_t min = data_[0][hash_functions_[0](item) % width_];
  for (size_t i = 1; i < depth_; i++) {
    uint32_t result = data_[i][hash_functions_[i](item) % width_];
    if (result < min) {
      min = result;
    }
  }
  return min;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  if (width_ <= 0 || depth_ <= 0) {
    std::cout << "Fuck god!!!" << std::endl;
  }

  for (auto &v : data_) {
    for (auto &i : v) {
      i = 0;
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  if (width_ <= 0 || depth_ <= 0) {
    std::cout << "Fuck god!!!" << std::endl;
  }
  std::vector<std::pair<KeyType, uint32_t>> result(candidates.size());
  for (const auto &key : candidates) {
    result.emplace_back(key, Count(key));
  }
  std::sort(result.begin(), result.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
  if (k < result.size()) {
    result.resize(k);
  }
  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
