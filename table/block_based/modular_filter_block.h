//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/block_based/filter_block_reader_common.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

// A ModularFilterBlockBuilder is used to construct several modular filters for
// a particular Table.  It generates multiple strings of which each is stored as
// a special block in the Table.
// The format of full filter block is:
// +----------------------------------------------------------------+
// |              modular filter (1) for all keys in sst file       |
// |              modular filter (2) for all keys in sst file       |
// |                           ...                                  |
// +----------------------------------------------------------------+
//
class ModularFilterBlockBuilder : public FilterBlockBuilder {
 public:
  explicit ModularFilterBlockBuilder(const SliceTransform* prefix_extractor,
                                     bool whole_key_filtering,
                                     const FilterBuildingContext& context,
                                     /* const uint32_t partition_size,*/
                                     double bpk, bool prefetch);
  // No copying allowed
  ModularFilterBlockBuilder(const ModularFilterBlockBuilder&) = delete;
  void operator=(const ModularFilterBlockBuilder&) = delete;

  ~ModularFilterBlockBuilder() {
    start_.clear();
    entries_.clear();
  }

  virtual bool IsBlockBased() override { return false; }
  virtual void StartBlock(uint64_t /*block_offset*/) override {}
  virtual void Add(const Slice& key) override;
  virtual size_t NumAdded() const override { return num_added_; }
  virtual Slice Finish(const BlockHandle& tmp, Status* status) override;
  virtual void ResetFilterBuilder(float bpk) override;
  using FilterBlockBuilder::Finish;

 protected:
  virtual void AddKey(const Slice& key);
  // std::unique_ptr<FilterBitsBuilder> filter_bits_builder_;
  virtual void Reset();
  void AddPrefix(const Slice& key);
  const SliceTransform* prefix_extractor() { return prefix_extractor_; }

 private:
  FilterBuildingContext context_;
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;
  bool last_whole_key_recorded_;
  std::string last_whole_key_str_;
  bool last_prefix_recorded_;
  std::string last_prefix_str_;

  uint32_t num_added_;
  double bpk_;
  bool prefetch_;
  std::vector<size_t> start_;
  std::string entries_;
  // std::unique_ptr<const char[]> prefetch_filter_gc;
  std::unique_ptr<const char[]> filter_gc;
  // std::unique_ptr<const char[]> filter_data_;
  // FilterBitsBuilder* prefetch_filter_bits_builder_;
  FilterBitsBuilder* filter_bits_builder_;
  std::vector<uint32_t> filter_offsets_;
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class ModularFilterBlockReader
    : public FilterBlockReaderCommon<ParsedFullFilterBlock> {
 public:
  ModularFilterBlockReader(
      const BlockBasedTable* t,
      CachableEntry<ParsedFullFilterBlock>&& filter_block,
      CachableEntry<ParsedFullFilterBlock>&& prefetch_filter_block);

  static std::unique_ptr<FilterBlockReader> Create(
      const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
      bool use_cache, bool prefetch, bool pin,
      BlockCacheLookupContext* lookup_context);

  bool IsBlockBased() override { return false; }

  bool KeyMayMatch(const Slice& key, const SliceTransform* prefix_extractor,
                   uint64_t block_offset, const bool no_io,
                   const Slice* const const_ikey_ptr, GetContext* get_context,
                   BlockCacheLookupContext* lookup_context) override;

  bool PrefixMayMatch(const Slice& prefix,
                      const SliceTransform* prefix_extractor,
                      uint64_t block_offset, const bool no_io,
                      const Slice* const const_ikey_ptr,
                      GetContext* get_context,
                      BlockCacheLookupContext* lookup_context) override;

  void KeysMayMatch(MultiGetRange* range,
                    const SliceTransform* prefix_extractor,
                    uint64_t block_offset, const bool no_io,
                    BlockCacheLookupContext* lookup_context) override;

  void PrefixesMayMatch(MultiGetRange* range,
                        const SliceTransform* prefix_extractor,
                        uint64_t block_offset, const bool no_io,
                        BlockCacheLookupContext* lookup_context) override;
  size_t ApproximateMemoryUsage() const override;
  bool RangeMayExist(const Slice* iterate_upper_bound, const Slice& user_key,
                     const SliceTransform* prefix_extractor,
                     const Comparator* comparator,
                     const Slice* const const_ikey_ptr, bool* filter_checked,
                     bool need_upper_bound_check, bool no_io,
                     BlockCacheLookupContext* lookup_context) override;
  void NextMayMatch(const Slice& entry, bool no_io, GetContext* get_context,
                    BlockCacheLookupContext* lookup_context, bool& next_match,
                    bool* in_cache_p) const;

 private:
  bool MayMatch(const Slice& entry, bool no_io, GetContext* get_context,
                BlockCacheLookupContext* lookup_context) const;
  void MayMatch(MultiGetRange* range, bool no_io,
                const SliceTransform* prefix_extractor,
                BlockCacheLookupContext* lookup_context) const;
  bool IsFilterCompatible(const Slice* iterate_upper_bound, const Slice& prefix,
                          const Comparator* comparator) const;

 private:
  bool full_length_enabled_;
  size_t prefix_extractor_full_length_;
};

}  // namespace ROCKSDB_NAMESPACE
