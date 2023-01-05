//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/modular_filter_block.h"

#include <array>
#include <functional>
#include <thread>

#include "monitoring/perf_context_imp.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

/*  avoid multiple hashing
void TransformKey(const Slice & key, size_t index, bool prefetch, Slice &
new_key){

    std::string* result_ = new std::string();
    if(index == 0 && prefetch){
        new_key = key.ToString();
        return;
    }

    new_key.clear();
    std::string new_key_str = "";
    if(prefetch){
        for(size_t i = 0; i < key.size(); i++){
          new_key_str.push_back(key[i] + index);
        }
    }else{
        for(size_t i = 0; i < key.size(); i++){
          new_key_str.push_back(key[i] - index - 1);
        }
    }
    new_key = Slice(new_key_str);

}
*/

ModularFilterBlockBuilder::ModularFilterBlockBuilder(
    const SliceTransform* _prefix_extractor, bool whole_key_filtering,
    const FilterBuildingContext& context,
    /*const uint32_t partition_size,*/
    double bpk, bool prefetch)
    : context_(context),
      prefix_extractor_(_prefix_extractor),
      whole_key_filtering_(whole_key_filtering),
      last_whole_key_recorded_(false),
      last_prefix_recorded_(false),
      num_added_(0),
      bpk_(bpk),
      prefetch_(prefetch) {
  filter_gc = std::unique_ptr<const char[]>(nullptr);
  filter_bits_builder_ =
      BloomFilterPolicy::GetBuilderFromContext(context, bpk, prefetch_);
}

void ModularFilterBlockBuilder::ResetFilterBuilder(float bpk) {
  filter_bits_builder_->ResetBPK(bpk);
}

void ModularFilterBlockBuilder::Add(const Slice& key) {
  num_added_++;
  bool add_prefix = true;
  add_prefix = prefix_extractor_ && prefix_extractor_->InDomain(key);
  if (whole_key_filtering_) {
    if (!add_prefix) {
      AddKey(key);
    } else {
      // if both whole_key and prefix are added to bloom then we will have whole
      // key and prefix addition being interleaved and thus cannot rely on the
      // bits builder to properly detect the duplicates by comparing with the
      // last item.
      Slice last_whole_key = Slice(last_whole_key_str_);
      if (!last_whole_key_recorded_ || last_whole_key.compare(key) != 0) {
        AddKey(key);
        last_whole_key_recorded_ = true;
        last_whole_key_str_.assign(key.data(), key.size());
      }
    }
  }
  if (add_prefix) {
    AddPrefix(key);
  }
}

// Add key to filter if needed
inline void ModularFilterBlockBuilder::AddKey(const Slice& key) {
  filter_bits_builder_->AddKey(key);
}

// Add prefix to filter if needed
void ModularFilterBlockBuilder::AddPrefix(const Slice& key) {
  Slice prefix = prefix_extractor_->Transform(key);
  if (whole_key_filtering_) {
    // if both whole_key and prefix are added to bloom then we will have whole
    // key and prefix addition being interleaved and thus cannot rely on the
    // bits builder to properly detect the duplicates by comparing with the last
    // item.
    Slice last_prefix = Slice(last_prefix_str_);
    if (!last_prefix_recorded_ || last_prefix.compare(prefix) != 0) {
      AddKey(prefix);
      last_prefix_recorded_ = true;
      last_prefix_str_.assign(prefix.data(), prefix.size());
    }
  } else {
    AddKey(prefix);
  }
}

void ModularFilterBlockBuilder::Reset() {
  last_whole_key_recorded_ = false;
  last_prefix_recorded_ = false;
  start_.clear();
  entries_.clear();
}

Slice ModularFilterBlockBuilder::Finish(const BlockHandle& /*tmp*/,
                                        Status* status) {
  Reset();
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (num_added_ != 0) {
    num_added_ = 0;
    return filter_bits_builder_->Finish(&filter_gc);
  } else {
    return Slice();
  }
}

ModularFilterBlockReader::ModularFilterBlockReader(
    const BlockBasedTable* t,
    CachableEntry<ParsedFullFilterBlock>&& filter_block,
    CachableEntry<ParsedFullFilterBlock>&& prefetch_filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block),
                              std::move(prefetch_filter_block)) {
  const SliceTransform* const prefix_extractor = table_prefix_extractor();
  if (prefix_extractor) {
    full_length_enabled_ =
        prefix_extractor->FullLengthEnabled(&prefix_extractor_full_length_);
  }
  assert(table());
  assert(table()->get_rep());
  assert(table()->get_rep()->filter_policy);
}

bool ModularFilterBlockReader::KeyMayMatch(
    const Slice& key, const SliceTransform* /*prefix_extractor*/,
    uint64_t block_offset, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!whole_key_filtering()) {
    return true;
  }
  return MayMatch(key, no_io, get_context, lookup_context);
}

void ModularFilterBlockReader::NextMayMatch(
    const Slice& entry, bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, bool& next_match,
    bool* in_cache_p) const {
  CachableEntry<ParsedFullFilterBlock> filter_block;
  if (table()->get_rep()->filter_handle.IsNull()) return;
  next_match = true;

  const Status s = GetOrReadFilterBlock(no_io, get_context, lookup_context,
                                        &filter_block, false, in_cache_p);
  if (!s.ok()) {
    IGNORE_STATUS_IF_ERROR(s);
    return;
  }

  if (!filter_block.GetValue()) {
    next_match = true;
    return;
  }

  FilterBitsReader* filter_bits_reader =
      filter_block.GetValue()->filter_bits_reader();
  if (filter_bits_reader) {
    next_match = filter_bits_reader->MayMatch(entry);
  }
}

std::unique_ptr<FilterBlockReader> ModularFilterBlockReader::Create(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    bool use_cache, bool prefetch, bool pin,
    BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<ParsedFullFilterBlock> filter_block;
  CachableEntry<ParsedFullFilterBlock> prefetch_filter_block;
  if (prefetch || !use_cache) {
    ModularFilterReadType mfilter_read_type =
        table->get_rep()->mfilter_read_type;
    bool in_cache = false;
    bool* in_cache_p = &in_cache;
    if (!table->get_rep()->prefetch_filter_handle.IsNull() &&
        (mfilter_read_type == ModularFilterReadType::kFirstFilterBlock ||
         mfilter_read_type == ModularFilterReadType::kBothFilterBlocks)) {
      const Status prefetch_s =
          ReadFilterBlock(table, prefetch_buffer, ReadOptions(), use_cache,
                          nullptr /* get_context */, lookup_context,
                          &prefetch_filter_block, true, in_cache_p);
      if (!prefetch_s.ok()) {
        IGNORE_STATUS_IF_ERROR(prefetch_s);
        return std::unique_ptr<FilterBlockReader>();
      }
    }

    if (mfilter_read_type == ModularFilterReadType::kSecondFilterBlock ||
        mfilter_read_type == ModularFilterReadType::kBothFilterBlocks) {
      if (!table->get_rep()->filter_handle.IsNull()) {
        /// not cache the second filter block
        const Status s =
            ReadFilterBlock(table, prefetch_buffer, ReadOptions(), use_cache,
                            nullptr /* get_context */, lookup_context,
                            &filter_block, in_cache_p);
        if (!s.ok()) {
          IGNORE_STATUS_IF_ERROR(s);
          return std::unique_ptr<FilterBlockReader>();
        }
      }
    }

    if (use_cache && !pin) {
      filter_block.Reset();
      prefetch_filter_block.Reset();
    }
  }

  return std::unique_ptr<FilterBlockReader>(new ModularFilterBlockReader(
      table, std::move(filter_block), std::move(prefetch_filter_block)));
}

bool ModularFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const SliceTransform* /* prefix_extractor */,
    uint64_t block_offset, const bool no_io,
    const Slice* const /*const_ikey_ptr*/, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  return MayMatch(prefix, no_io, get_context, lookup_context);
}

bool ModularFilterBlockReader::MayMatch(
    const Slice& entry, bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) const {
  CachableEntry<ParsedFullFilterBlock> prefetch_filter_block;

  assert(table());
  assert(table()->get_rep());

  bool prefetch_match = true;
  bool next_match = true;
  ModularFilterReadType mfilter_read_type =
      table()->get_rep()->mfilter_read_type;
  bool require_next_match =
      mfilter_read_type == ModularFilterReadType::kSecondFilterBlock ||
      mfilter_read_type == ModularFilterReadType::kBothFilterBlocks;
  bool concurrent_load = table()->get_rep()->table_options.concurrent_load;

  bool in_cache = false;
  bool* in_cache_p = &in_cache;

  port::Thread next_match_thread;
  if (require_next_match && concurrent_load) {
    next_match_thread = port::Thread(
        &ModularFilterBlockReader::NextMayMatch, this, std::ref(entry), no_io,
        std::ref(get_context), std::ref(lookup_context), std::ref(next_match),
        in_cache_p);
  }

  if ((mfilter_read_type == ModularFilterReadType::kFirstFilterBlock ||
       mfilter_read_type == ModularFilterReadType::kBothFilterBlocks) &&
      !table()->get_rep()->prefetch_filter_handle.IsNull()) {
    const Status prefetch_s =
        GetOrReadFilterBlock(no_io, get_context, lookup_context,
                             &prefetch_filter_block, true, in_cache_p);
    if (!prefetch_s.ok()) {
      IGNORE_STATUS_IF_ERROR(prefetch_s);
      return true;
    }

    assert(prefetch_filter_block.GetValue());

    FilterBitsReader* const prefetch_filter_bits_reader =
        prefetch_filter_block.GetValue()->filter_bits_reader();
    if (prefetch_filter_bits_reader) {
      prefetch_match = prefetch_filter_bits_reader->MayMatch(entry);
    }
  }

  if (require_next_match && prefetch_match) {
    if (concurrent_load) {
      next_match_thread.join();
    } else {
      NextMayMatch(entry, no_io, get_context, lookup_context, next_match,
                   in_cache_p);
    }
  }
  bool match = prefetch_match && next_match;
  if (match) {
    PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
  } else {
    PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
  }
  return match;  // remain the same with block_based filter
}

void ModularFilterBlockReader::KeysMayMatch(
    MultiGetRange* range, const SliceTransform* /*prefix_extractor*/,
    uint64_t block_offset, const bool no_io,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  if (!whole_key_filtering()) {
    // Simply return. Don't skip any key - consider all keys as likely to be
    // present
    return;
  }
  MayMatch(range, no_io, nullptr, lookup_context);
}

void ModularFilterBlockReader::PrefixesMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    uint64_t block_offset, const bool no_io,
    BlockCacheLookupContext* lookup_context) {
#ifdef NDEBUG
  (void)block_offset;
#endif
  assert(block_offset == kNotValid);
  MayMatch(range, no_io, prefix_extractor, lookup_context);
}

void ModularFilterBlockReader::MayMatch(
    MultiGetRange* range, bool no_io, const SliceTransform* prefix_extractor,
    BlockCacheLookupContext* lookup_context) const {
  CachableEntry<ParsedFullFilterBlock> prefetch_filter_block;

  assert(table());
  assert(table()->get_rep());

  ModularFilterReadType mfilter_read_type =
      table()->get_rep()->mfilter_read_type;
  bool require_next_match =
      mfilter_read_type == ModularFilterReadType::kSecondFilterBlock ||
      mfilter_read_type == ModularFilterReadType::kBothFilterBlocks;

  // We need to use an array instead of autovector for may_match since
  // &may_match[0] doesn't work for autovector<bool> (compiler error). So
  // declare both keys and may_match as arrays, which is also slightly less
  // expensive compared to autovector
  std::array<Slice*, MultiGetContext::MAX_BATCH_SIZE> keys;
  std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match = {{true}};
  autovector<Slice, MultiGetContext::MAX_BATCH_SIZE> prefixes;
  int num_keys = 0;
  MultiGetRange filter_range(*range, range->begin(), range->end());
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!prefix_extractor) {
      keys[num_keys++] = &iter->ukey_without_ts;
    } else if (prefix_extractor->InDomain(iter->ukey_without_ts)) {
      prefixes.emplace_back(prefix_extractor->Transform(iter->ukey_without_ts));
      keys[num_keys++] = &prefixes.back();
    } else {
      filter_range.SkipKey(iter);
    }
  }

  bool prefetch_match = false;

  if ((mfilter_read_type == ModularFilterReadType::kFirstFilterBlock ||
       mfilter_read_type == ModularFilterReadType::kBothFilterBlocks) &&
      !table()->get_rep()->prefetch_filter_handle.IsNull()) {
    const Status prefetch_s =
        GetOrReadFilterBlock(no_io, range->begin()->get_context, lookup_context,
                             &prefetch_filter_block, true);
    if (!prefetch_s.ok()) {
      IGNORE_STATUS_IF_ERROR(prefetch_s);
      return;
    }

    assert(prefetch_filter_block.GetValue());
    FilterBitsReader* const prefetch_filter_bits_reader =
        prefetch_filter_block.GetValue()->filter_bits_reader();

    if (prefetch_filter_bits_reader) {
      prefetch_filter_bits_reader->MayMatch(num_keys, &keys[0], &may_match[0]);
    }
    int i = 0;
    for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
      if (may_match[i]) {
        prefetch_match = true;
        break;
      }
      ++i;
    }
  }
  if (prefetch_match && require_next_match &&
      !table()->get_rep()->filter_handle.IsNull()) {
    CachableEntry<ParsedFullFilterBlock> filter_block;
    const Status s = GetOrReadFilterBlock(no_io, range->begin()->get_context,
                                          lookup_context, &filter_block, false);
    if (!s.ok()) {
      IGNORE_STATUS_IF_ERROR(s);
      return;
    }

    assert(filter_block.GetValue());

    FilterBitsReader* const filter_bits_reader =
        filter_block.GetValue()->filter_bits_reader();
    if (filter_bits_reader) {
      filter_bits_reader->MayMatch(num_keys, &keys[0], &may_match[0]);
    }
  }

  int i = 0;
  for (auto iter = filter_range.begin(); iter != filter_range.end(); ++iter) {
    if (!may_match[i]) {
      // Update original MultiGet range to skip this key. The filter_range
      // was temporarily used just to skip keys not in prefix_extractor domain
      range->SkipKey(iter);
      PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
    } else {
      // PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
      PerfContext* perf_ctx = get_perf_context();
      perf_ctx->bloom_sst_hit_count++;
    }
    ++i;
  }
}

size_t ModularFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<ModularFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
}

bool ModularFilterBlockReader::RangeMayExist(
    const Slice* iterate_upper_bound, const Slice& user_key,
    const SliceTransform* prefix_extractor, const Comparator* comparator,
    const Slice* const const_ikey_ptr, bool* filter_checked,
    bool need_upper_bound_check, bool no_io,
    BlockCacheLookupContext* lookup_context) {
  if (!prefix_extractor || !prefix_extractor->InDomain(user_key)) {
    *filter_checked = false;
    return true;
  }
  Slice prefix = prefix_extractor->Transform(user_key);
  if (need_upper_bound_check &&
      !IsFilterCompatible(iterate_upper_bound, prefix, comparator)) {
    *filter_checked = false;
    return true;
  } else {
    *filter_checked = true;
    return PrefixMayMatch(prefix, prefix_extractor, kNotValid, no_io,
                          const_ikey_ptr, /* get_context */ nullptr,
                          lookup_context);
  }
}

bool ModularFilterBlockReader::IsFilterCompatible(
    const Slice* iterate_upper_bound, const Slice& prefix,
    const Comparator* comparator) const {
  // Try to reuse the bloom filter in the SST table if prefix_extractor in
  // mutable_cf_options has changed. If range [user_key, upper_bound) all
  // share the same prefix then we may still be able to use the bloom filter.
  const SliceTransform* const prefix_extractor = table_prefix_extractor();
  if (iterate_upper_bound != nullptr && prefix_extractor) {
    if (!prefix_extractor->InDomain(*iterate_upper_bound)) {
      return false;
    }
    Slice upper_bound_xform = prefix_extractor->Transform(*iterate_upper_bound);
    // first check if user_key and upper_bound all share the same prefix
    if (!comparator->Equal(prefix, upper_bound_xform)) {
      // second check if user_key's prefix is the immediate predecessor of
      // upper_bound and have the same length. If so, we know for sure all
      // keys in the range [user_key, upper_bound) share the same prefix.
      // Also need to make sure upper_bound are full length to ensure
      // correctness
      if (!full_length_enabled_ ||
          iterate_upper_bound->size() != prefix_extractor_full_length_ ||
          !comparator->IsSameLengthImmediateSuccessor(prefix,
                                                      *iterate_upper_bound)) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

}  // namespace ROCKSDB_NAMESPACE
