//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <utility>
#include <vector>

namespace bustub {

FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

auto FrameHeader::GetData() const -> const char * { return data_.data(); }

auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_ = INVALID_PAGE_ID;
}

BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  std::scoped_lock latch(*bpm_latch_);
  next_page_id_.store(0);
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

auto BufferPoolManager::NewPage() -> page_id_t { return next_page_id_.fetch_add(1); }

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch(*bpm_latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    auto &frame = frames_[frame_id];
    if (frame->pin_count_.load() > 0) {
      return false;
    }
    replacer_->Remove(frame_id);
    page_table_.erase(it);
    frame->Reset();
    free_frames_.push_back(frame_id);
  }
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id < 0) {
    return std::nullopt;
  }
  std::shared_ptr<FrameHeader> frame;
  {
    std::scoped_lock latch(*bpm_latch_);
    frame = PinPage(page_id, access_type);
    if (frame == nullptr) {
      return std::nullopt;
    }
  }
  return WritePageGuard(page_id, std::move(frame), replacer_, bpm_latch_, disk_scheduler_);
}

auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  if (page_id < 0) {
    return std::nullopt;
  }
  std::shared_ptr<FrameHeader> frame;
  {
    std::scoped_lock latch(*bpm_latch_);
    frame = PinPage(page_id, access_type);
    if (frame == nullptr) {
      return std::nullopt;
    }
  }
  return ReadPageGuard(page_id, std::move(frame), replacer_, bpm_latch_, disk_scheduler_);
}

auto BufferPoolManager::AcquireFrame() -> std::optional<frame_id_t> {
  if (!free_frames_.empty()) {
    frame_id_t frame_id = free_frames_.front();
    free_frames_.pop_front();
    return frame_id;
  }

  auto victim = replacer_->Evict();
  if (!victim.has_value()) {
    return std::nullopt;
  }
  auto &frame = frames_[*victim];
  if (frame->is_dirty_) {
    DoDiskIo(true, *frame, frame->page_id_);
  }
  page_table_.erase(frame->page_id_);
  frame->Reset();
  return victim;
}

auto BufferPoolManager::PinPage(page_id_t page_id, AccessType access_type) -> std::shared_ptr<FrameHeader> {
  frame_id_t frame_id;
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    frame_id = it->second;
  } else {
    auto free_frame = AcquireFrame();
    if (!free_frame.has_value()) {
      return nullptr;
    }
    frame_id = *free_frame;
    auto &frame = frames_[frame_id];
    DoDiskIo(false, *frame, page_id);
    frame->page_id_ = page_id;
    page_table_[page_id] = frame_id;
  }

  auto &frame = frames_[frame_id];
  frame->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  return frame;
}

void BufferPoolManager::DoDiskIo(bool is_write, FrameHeader &frame, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back({is_write, frame.GetDataMut(), page_id, std::move(promise)});
  disk_scheduler_->Schedule(requests);
  future.get();
}

auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  std::scoped_lock latch(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto &frame = frames_[it->second];
  if (frame->is_dirty_) {
    DoDiskIo(true, *frame, page_id);
    frame->is_dirty_ = false;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::shared_ptr<FrameHeader> frame;
  {
    std::scoped_lock latch(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }
    frame = frames_[it->second];
    frame->pin_count_.fetch_add(1);
    replacer_->SetEvictable(frame->frame_id_, false);
  }

  {
    std::shared_lock frame_latch(frame->rwlatch_);
    if (frame->is_dirty_) {
      DoDiskIo(true, *frame, page_id);
      frame->is_dirty_ = false;
    }
  }

  std::scoped_lock latch(*bpm_latch_);
  if (frame->pin_count_.fetch_sub(1) == 1) {
    replacer_->SetEvictable(frame->frame_id_, true);
  }
  return true;
}

void BufferPoolManager::FlushAllPagesUnsafe() {
  std::scoped_lock latch(*bpm_latch_);
  for (const auto &[page_id, frame_id] : page_table_) {
    auto &frame = frames_[frame_id];
    if (frame->is_dirty_) {
      DoDiskIo(true, *frame, page_id);
      frame->is_dirty_ = false;
    }
  }
}

void BufferPoolManager::FlushAllPages() {
  std::vector<page_id_t> page_ids;
  {
    std::scoped_lock latch(*bpm_latch_);
    page_ids.reserve(page_table_.size());
    for (const auto &entry : page_table_) {
      page_ids.push_back(entry.first);
    }
  }
  for (auto page_id : page_ids) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock latch(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  return frames_[it->second]->pin_count_.load();
}

}  // namespace bustub
