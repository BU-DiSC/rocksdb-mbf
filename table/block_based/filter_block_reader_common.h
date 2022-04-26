//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <cassert>
#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"

namespace ROCKSDB_NAMESPACE {

class BlockBasedTable;
class FilePrefetchBuffer;

// Encapsulates common functionality for the various filter block reader
// implementations. Provides access to the filter block regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
template <typename TBlocklike>
class FilterBlockReaderCommon : public FilterBlockReader {
 public:
  // added for modular filter
  FilterBlockReaderCommon(const BlockBasedTable* t,
                          CachableEntry<TBlocklike>&& filter_block,
                          CachableEntry<TBlocklike>&& prefetch_filter_block) 
      : table_(t), filter_block_(std::move(filter_block)), prefetch_filter_block_(std::move(prefetch_filter_block)) {
    assert(table_);
  }
  FilterBlockReaderCommon(const BlockBasedTable* t,
                          CachableEntry<TBlocklike>&& filter_block)
      : table_(t), filter_block_(std::move(filter_block)) {
    assert(table_);
  }

 protected:
  static Status ReadFilterBlock(const BlockBasedTable* table,
                                FilePrefetchBuffer* prefetch_buffer,
                                const ReadOptions& read_options, bool use_cache,
                                GetContext* get_context,
                                BlockCacheLookupContext* lookup_context,
                                CachableEntry<TBlocklike>* filter_block,
                                 bool prefetch_filter=false, bool* in_cache=nullptr); /* prefetch_filter arg added for modular filter */

  const BlockBasedTable* table() const { return table_; }
  const SliceTransform* table_prefix_extractor() const;
  bool whole_key_filtering() const;
  bool cache_filter_blocks() const;

  Status GetOrReadFilterBlock(bool no_io, GetContext* get_context,
                              BlockCacheLookupContext* lookup_context,
                              CachableEntry<TBlocklike>* filter_block,
                              bool prefetch_filter=false, bool* in_cache=nullptr) const; /* prefetch_filter arg added for modular filter */) const;

  size_t ApproximateFilterBlockMemoryUsage() const;

 private:
  const BlockBasedTable* table_;
  CachableEntry<TBlocklike> filter_block_;
  CachableEntry<TBlocklike> prefetch_filter_block_; // added for modular filter
};

}  // namespace ROCKSDB_NAMESPACE
