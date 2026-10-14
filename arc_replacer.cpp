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
#include <algorithm>
#include <optional>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  ArcStatus first = mru_.size() >= mru_target_size_ ? ArcStatus::MRU : ArcStatus::MFU;
  ArcStatus second = first == ArcStatus::MRU ? ArcStatus::MFU : ArcStatus::MRU;
  if (auto victim = TryEvictFrom(first); victim.has_value()) {
    return victim;
  }
  return TryEvictFrom(second);
}

auto ArcReplacer::ListOf(ArcStatus status) -> std::list<int32_t> & {
  switch (status) {
    case ArcStatus::MRU:
      return mru_;
    case ArcStatus::MFU:
      return mfu_;
    case ArcStatus::MRU_GHOST:
      return mru_ghost_;
    case ArcStatus::MFU_GHOST:
      return mfu_ghost_;
  }
  UNREACHABLE("invalid arc status");
}

auto ArcReplacer::TryEvictFrom(ArcStatus status) -> std::optional<frame_id_t> {
  auto &list = ListOf(status);
  for (auto it = list.rbegin(); it != list.rend(); ++it) {
    auto entry = alive_map_[*it];
    if (!entry->evictable_) {
      continue;
    }
    frame_id_t frame_id = entry->frame_id_;
    list.erase(std::next(it).base());
    alive_map_.erase(frame_id);
    curr_size_--;

    ArcStatus ghost_status = status == ArcStatus::MRU ? ArcStatus::MRU_GHOST : ArcStatus::MFU_GHOST;
    auto &ghost_list = ListOf(ghost_status);
    ghost_list.push_front(entry->page_id_);
    entry->arc_status_ = ghost_status;
    entry->evictable_ = false;
    entry->iter_ = ghost_list.begin();
    ghost_map_[entry->page_id_] = entry;
    return frame_id;
  }
  return std::nullopt;
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lock(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "invalid frame id");
  }

  if (auto it = alive_map_.find(frame_id); it != alive_map_.end()) {
    auto entry = it->second;
    ListOf(entry->arc_status_).erase(entry->iter_);
    mfu_.push_front(frame_id);
    entry->iter_ = mfu_.begin();
    entry->arc_status_ = ArcStatus::MFU;
    return;
  }

  if (auto it = ghost_map_.find(page_id); it != ghost_map_.end()) {
    auto entry = it->second;
    size_t mru_ghost_size = mru_ghost_.size();
    size_t mfu_ghost_size = mfu_ghost_.size();
    if (entry->arc_status_ == ArcStatus::MRU_GHOST) {
      size_t delta = mru_ghost_size >= mfu_ghost_size ? 1 : mfu_ghost_size / mru_ghost_size;
      mru_target_size_ = std::min(mru_target_size_ + delta, replacer_size_);
    } else {
      size_t delta = mfu_ghost_size >= mru_ghost_size ? 1 : mru_ghost_size / mfu_ghost_size;
      mru_target_size_ = mru_target_size_ > delta ? mru_target_size_ - delta : 0;
    }
    ListOf(entry->arc_status_).erase(entry->iter_);
    ghost_map_.erase(it);

    mfu_.push_front(frame_id);
    entry->frame_id_ = frame_id;
    entry->evictable_ = false;
    entry->arc_status_ = ArcStatus::MFU;
    entry->iter_ = mfu_.begin();
    alive_map_[frame_id] = entry;
    return;
  }

  if (mru_.size() + mru_ghost_.size() >= replacer_size_) {
    if (!mru_ghost_.empty()) {
      ghost_map_.erase(mru_ghost_.back());
      mru_ghost_.pop_back();
    }
  } else if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() >= 2 * replacer_size_) {
    if (!mfu_ghost_.empty()) {
      ghost_map_.erase(mfu_ghost_.back());
      mfu_ghost_.pop_back();
    }
  }
  mru_.push_front(frame_id);
  auto entry = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  entry->iter_ = mru_.begin();
  alive_map_[frame_id] = entry;
}

void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "invalid frame id");
  }
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }
  auto &entry = it->second;
  if (entry->evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!entry->evictable_ && set_evictable) {
    curr_size_++;
  }
  entry->evictable_ = set_evictable;
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }
  auto entry = it->second;
  if (!entry->evictable_) {
    throw Exception(ExceptionType::INVALID, "cannot remove a non-evictable frame");
  }

  ListOf(entry->arc_status_).erase(entry->iter_);
  alive_map_.erase(it);
  curr_size_--;
}

auto ArcReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub
