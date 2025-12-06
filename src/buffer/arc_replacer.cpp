// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "fmt/xchar.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::optional<frame_id_t> removed;
  if (mru_.size() < mru_target_size_) {
    // try to remove from mfu
    removed = TryGetEvictableFrom(mfu_);
    if (removed != std::nullopt) {
      const auto page_id = alive_map_[removed.value()]->page_id_;
      Move2Ghost(removed.value(), page_id, mfu_ghost_, mfu_ghost_map_);
      RemoveFrom(removed.value(), mfu_, mfu_map_);
    } else {
      // remove from mru
      removed = TryGetEvictableFrom(mru_);
      if (removed == std::nullopt) goto fail;
      const auto page_id = alive_map_[removed.value()]->page_id_;
      Move2Ghost(removed.value(), page_id, mru_ghost_, mru_ghost_map_);
      RemoveFrom(removed.value(), mru_, mru_map_);
    }
  } else {
    // try to remove from mru
    removed = TryGetEvictableFrom(mru_);
    if (removed != std::nullopt) {
      const auto page_id = alive_map_[removed.value()]->page_id_;
      Move2Ghost(removed.value(), page_id, mru_ghost_, mru_ghost_map_);
      RemoveFrom(removed.value(), mru_, mru_map_);
    } else {
      // remove from mfu
      removed = TryGetEvictableFrom(mfu_);
      if (removed == std::nullopt) goto fail;
      const auto page_id = alive_map_[removed.value()]->page_id_;
      Move2Ghost(removed.value(), page_id, mfu_ghost_, mfu_ghost_map_);
      RemoveFrom(removed.value(), mfu_, mfu_map_);
    }
  }
  --curr_size_;
  DumpState();
  return removed;

  fail:
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  if (LookUp(frame_id, page_id) != nullptr) {
    // hit alive
    Move2First(frame_id);
    DumpState();
    return;
  }

  if (LookUpGhost(frame_id, page_id) != nullptr) {
    // hit ghost
    auto it = mru_ghost_map_.find(page_id);
    if (it != mru_ghost_map_.end()) {
      // in mru_ghost
      if (mru_ghost_.size() >= mfu_ghost_.size()) {
        ++mru_target_size_;
      } else {
        mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
        mru_target_size_ = mru_target_size_ > replacer_size_ ? replacer_size_ : mru_target_size_;
      }
      std::cout << "hit ghost" << std::endl;
      MoveGhost2First(frame_id, page_id);
      DumpState();
      return;
    }

    it = mfu_ghost_map_.find(page_id);
    if (it != mfu_ghost_map_.end()) {
      // in mfu_ghost
      if (mfu_ghost_.size() >= mru_ghost_.size()) {
        --mru_target_size_;
      } else {
        mru_target_size_ -= mru_ghost_.size() / mfu_ghost_.size();
        mru_target_size_ = mru_target_size_ <= 0 ? 0 : mru_target_size_;
      }
      std::cout << "hit ghost" << std::endl;
      MoveGhost2First(frame_id, page_id);
      DumpState();
      return;
    }
    throw std::runtime_error("Unexpected state(2)");
  }

  // miss
  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    // kill last mru_ghost element
    const page_id_t last_page_id = mru_ghost_.back();
    mru_ghost_.pop_back();
    mru_ghost_map_.erase(last_page_id);
    ghost_map_.erase(last_page_id);

    // Added to mru's front
    mru_.emplace_front(frame_id);
    mru_map_[frame_id] = mru_.begin();
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);
  } else if (mru_.size() + mru_ghost_.size() < replacer_size_) {
    auto allSize = mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size();
    if (allSize == 2 * replacer_size_) {
      // kill last mfu_ghost element
      const page_id_t last_page_id = mfu_ghost_.back();
      mfu_ghost_.pop_back();
      mfu_ghost_map_.erase(last_page_id);
      ghost_map_.erase(last_page_id);
    }
    // Added to mru's front
    mru_.emplace_front(frame_id);
    mru_map_[frame_id] = mru_.begin();
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);
  } else {
    throw std::runtime_error("Unexpected state(3)");
  }
  ++curr_size_;
  DumpState();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    if (it->second->evictable_ == false && set_evictable == true) {
      ++curr_size_;
    } else if (it->second->evictable_ == true && set_evictable == false) {
      --curr_size_;
    }
    it->second->evictable_ = set_evictable;
  } else {
    throw std::runtime_error("Unexpected state(5)");
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  auto it = alive_map_.find(frame_id);
  if (it != alive_map_.end()) {
    if (it->second->evictable_ == false) {
      throw std::runtime_error("Unexpected state(6)");
    }
  } else {
    return;
  }
  --curr_size_;
  alive_map_.erase(frame_id);

  // remove from list
  auto it2 = mru_map_.find(frame_id);
  if (it2 != mru_map_.end()) {
    mru_.erase(it2->second);
    mru_map_.erase(it2);
    return;
  }
  it2 = mfu_map_.find(frame_id);
  if (it2 != mfu_map_.end()) {
    mfu_.erase(it2->second);
    mfu_map_.erase(it2);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { return curr_size_; }

auto ArcReplacer::LookUp(frame_id_t frame_id, page_id_t page_id) const -> std::shared_ptr<FrameStatus> {
  const auto it = alive_map_.find(frame_id);
  return (it != alive_map_.end() && it->second->page_id_ == page_id) ? it->second : nullptr;
}

auto ArcReplacer::LookUpGhost(frame_id_t frame_id, page_id_t page_id) const -> std::shared_ptr<FrameStatus> {
  const auto it = ghost_map_.find(page_id);
  return it != ghost_map_.end() ? it->second : nullptr;
}

void ArcReplacer::Move2First(frame_id_t frame_id) {
  auto it = mru_map_.find(frame_id);
  if (it != mru_map_.end()) {
    mru_.erase(it->second);
    mru_map_.erase(frame_id);

    mfu_.emplace_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();
    return;
  }

  it = mfu_map_.find(frame_id);
  if (it != mfu_map_.end()) {
    mfu_.erase(it->second);
    mfu_map_.erase(frame_id);

    mfu_.emplace_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();
    return;
  }
  throw std::runtime_error("Unexpected state(1)");
}

void ArcReplacer::MoveGhost2First(frame_id_t frame_id, page_id_t page_id) {
  auto it = mru_ghost_map_.find(page_id);
  if (it != mru_ghost_map_.end()) {
    // remove from mru_ghost list
    mru_ghost_.erase(it->second);
    // move to mru's front
    mfu_.emplace_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
    ++curr_size_;
    ghost_map_.erase(page_id);
    // remove from mru_ghost map
    mru_ghost_map_.erase(page_id);
    return;
  }

  it = mfu_ghost_map_.find(page_id);
  if (it != mfu_ghost_map_.end()) {
    // remove from mfu_ghost list
    mfu_ghost_.erase(it->second);
    // move to mru's front
    mfu_.emplace_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
    ++curr_size_;
    ghost_map_.erase(page_id);
    // remove from mfu_ghost map
    mfu_ghost_map_.erase(page_id);
    return;
  }
  throw std::runtime_error("Unexpected state(1)");
}

auto ArcReplacer::TryGetEvictableFrom(std::list<frame_id_t>& list_) -> std::optional<frame_id_t> {
  for (auto it = list_.rbegin(); it != list_.rend(); ++it) {
    frame_id_t frame_id_tmp = *it;
    if (alive_map_[frame_id_tmp]->evictable_) {
      return frame_id_tmp;
    }
  }
  return std::nullopt;
}

/*
 * Note:
 *  This function ensure the frame_id in the list and map
 *  Never use it when you don't know whether the frame_id in the list and map.
 */
void ArcReplacer::RemoveFrom(
  frame_id_t frame_id,
  std::list<frame_id_t> &l,
  std::unordered_map<page_id_t, std::list<page_id_t>::iterator> &mp
  ) {
  /*
   * 1. remove from the list
   * 2. remove from the map
   */
  const auto where = mp.find(frame_id)->second;
  l.erase(where);
  mp.erase(frame_id);
}

void ArcReplacer::Move2Ghost(
  frame_id_t frame_id,
  page_id_t page_id,
  std::list<page_id_t> &l,
  std::unordered_map<page_id_t, std::list<page_id_t>::iterator> &mp
  ) {
  if (ghost_map_.find(page_id) != ghost_map_.end()) {
    // the page_id already in ghost list
    throw std::runtime_error("Unexpected state(7)");
  }
  l.emplace_front(page_id);
  mp[page_id] = l.begin();
  ghost_map_[page_id] = std::move(alive_map_[frame_id]);
  alive_map_.erase(frame_id);
}

/*
 * Note: Used to debug
 */
void ArcReplacer::DumpState(bool debug) {
  if (debug) {
    std::cout << "mru_list: ";
    for (auto v: mru_) {
      std::cout << v << " ";
    }
    std::cout << std::endl;

    std::cout << "mfu_list: ";
    for (auto v: mfu_) {
      std::cout << v << " ";
    }
    std::cout << std::endl;

    std::cout << "mru_ghost_list: ";
    for (auto v: mru_ghost_) {
      std::cout << v << " ";
    }
    std::cout << std::endl;

    std::cout << "mfu_ghost_list: ";
    for (auto v: mfu_ghost_) {
      std::cout << v << " ";
    }
    std::cout << std::endl;

    std::cout << "current size = " << curr_size_ << std::endl;
    std::cout << std::endl;
  }
}

}  // namespace bustub
