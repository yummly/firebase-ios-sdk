/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Firestore/core/src/local/leveldb_document_overlay_cache.h"

#include <map>
#include <string>
#include <unordered_set>
#include <utility>

#include "Firestore/core/src/credentials/user.h"
#include "Firestore/core/src/local/leveldb_key.h"
#include "Firestore/core/src/local/leveldb_persistence.h"
#include "Firestore/core/src/local/local_serializer.h"
#include "Firestore/core/src/nanopb/message.h"
#include "Firestore/core/src/nanopb/reader.h"
#include "Firestore/core/src/util/hard_assert.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace firebase {
namespace firestore {
namespace local {

using credentials::User;
using model::DocumentKey;
using model::Mutation;
using model::ResourcePath;
using model::mutation::Overlay;
using model::mutation::OverlayHash;
using nanopb::Message;
using nanopb::StringReader;

LevelDbDocumentOverlayCache::LevelDbDocumentOverlayCache(
    const User& user, LevelDbPersistence* db, LocalSerializer* serializer)
    : db_(NOT_NULL(db)),
      serializer_(NOT_NULL(serializer)),
      user_id_(user.is_authenticated() ? user.uid() : "") {
}

absl::optional<Overlay> LevelDbDocumentOverlayCache::GetOverlay(
    const DocumentKey& key) const {
  const std::string leveldb_key_prefix =
      LevelDbDocumentOverlayKey::KeyPrefix(user_id_, key);

  auto it = db_->current_transaction()->NewIterator();
  it->Seek(leveldb_key_prefix);

  if (!(it->Valid() && absl::StartsWith(it->key(), leveldb_key_prefix))) {
    return absl::nullopt;
  }

  LevelDbDocumentOverlayKey decoded_key;
  HARD_ASSERT(decoded_key.Decode(it->key()));
  if (decoded_key.document_key() != key) {
    return absl::nullopt;
  }

  return ParseOverlay(decoded_key, it->value());
}

void LevelDbDocumentOverlayCache::SaveOverlays(
    int largest_batch_id, const MutationByDocumentKeyMap& overlays) {
  for (const auto& overlays_entry : overlays) {
    SaveOverlay(largest_batch_id, overlays_entry.first, overlays_entry.second);
  }
}

void LevelDbDocumentOverlayCache::RemoveOverlaysForBatchId(int batch_id) {
  // TODO(dconeybe) Implement an index so that this query can be performed
  // without requiring a full table scan.

  ForEachOverlay([&](absl::string_view encoded_key,
                     const LevelDbDocumentOverlayKey& decoded_key,
                     absl::string_view) {
    if (decoded_key.largest_batch_id() == batch_id) {
      db_->current_transaction()->Delete(encoded_key);
    }
  });
}

DocumentOverlayCache::OverlayByDocumentKeyMap
LevelDbDocumentOverlayCache::GetOverlays(const ResourcePath& collection,
                                         int since_batch_id) const {
  // TODO(dconeybe) Implement an index so that this query can be performed
  // without requiring a full table scan.

  OverlayByDocumentKeyMap result;

  const size_t immediate_children_path_length{collection.size() + 1};

  ForEachOverlay([&](absl::string_view,
                     const LevelDbDocumentOverlayKey& decoded_key,
                     absl::string_view encoded_mutation) {
    const DocumentKey key = decoded_key.document_key();
    if (!collection.IsPrefixOf(key.path())) {
      return;
    }
    // Documents from sub-collections
    if (key.path().size() != immediate_children_path_length) {
      return;
    }

    if (decoded_key.largest_batch_id() > since_batch_id) {
      result[key] = ParseOverlay(decoded_key, encoded_mutation);
    }
  });

  return result;
}

DocumentOverlayCache::OverlayByDocumentKeyMap
LevelDbDocumentOverlayCache::GetOverlays(const std::string& collection_group,
                                         int since_batch_id,
                                         std::size_t count) const {
  // TODO(dconeybe) Implement an index so that this query can be performed
  // without requiring a full table scan.

  // Load ALL overlays for the given `collection_group` whose largest_batch_id
  // are greater than the given `since_batch_id`. By using a `std::map` keyed
  // by largest_batch_id, the loop below can iterate over it ordered by
  // largest_batch_id.
  std::map<int, std::unordered_set<Overlay, OverlayHash>> overlays_by_batch_id;
  ForEachOverlay([&](absl::string_view,
                     const LevelDbDocumentOverlayKey& decoded_key,
                     absl::string_view encoded_mutation) {
    if (decoded_key.largest_batch_id() <= since_batch_id) {
      return;
    }
    if (decoded_key.document_key().HasCollectionId(collection_group)) {
      overlays_by_batch_id[decoded_key.largest_batch_id()].emplace(
          ParseOverlay(decoded_key, encoded_mutation));
    }
  });

  // Trim down the overlays loaded above to respect the given `count`, and
  // return them.
  //
  // Note that, as documented, all overlays for the largest_batch_id that pushes
  // the size of the result set above the given `count` will be returned, even
  // though this likely means that the size of the result set will be strictly
  // greater than the given `count`.
  OverlayByDocumentKeyMap result;
  for (auto& overlays_by_batch_id_entry : overlays_by_batch_id) {
    for (auto& overlay : overlays_by_batch_id_entry.second) {
      DocumentKey key = overlay.key();
      result[key] = std::move(overlay);
    }
    if (result.size() >= count) {
      break;
    }
  }

  return result;
}

Overlay LevelDbDocumentOverlayCache::ParseOverlay(
    const LevelDbDocumentOverlayKey& key,
    absl::string_view encoded_mutation) const {
  StringReader reader{encoded_mutation};
  auto maybe_message = Message<google_firestore_v1_Write>::TryParse(&reader);
  Mutation mutation = serializer_->DecodeMutation(&reader, *maybe_message);
  if (!reader.ok()) {
    HARD_FAIL("Mutation proto failed to parse: %s", reader.status().ToString());
  }
  return Overlay(key.largest_batch_id(), std::move(mutation));
}

void LevelDbDocumentOverlayCache::SaveOverlay(int largest_batch_id,
                                              const DocumentKey& key,
                                              const Mutation& mutation) {
  DeleteOverlay(key);
  const std::string leveldb_key =
      LevelDbDocumentOverlayKey::Key(user_id_, key, largest_batch_id);
  auto serialized_mutation = serializer_->EncodeMutation(mutation);
  db_->current_transaction()->Put(leveldb_key, serialized_mutation);
}

void LevelDbDocumentOverlayCache::DeleteOverlay(const model::DocumentKey& key) {
  const std::string leveldb_key_prefix =
      LevelDbDocumentOverlayKey::KeyPrefix(user_id_, key);
  auto it = db_->current_transaction()->NewIterator();
  for (it->Seek(leveldb_key_prefix);
       it->Valid() && absl::StartsWith(it->key(), leveldb_key_prefix);
       it->Next()) {
    LevelDbDocumentOverlayKey decoded_key;
    HARD_ASSERT(decoded_key.Decode(it->key()));
    if (decoded_key.document_key() == key) {
      db_->current_transaction()->Delete(it->key());
    }
  }
}

void LevelDbDocumentOverlayCache::ForEachOverlay(
    std::function<void(absl::string_view encoded_key,
                       const LevelDbDocumentOverlayKey& decoded_key,
                       absl::string_view encoded_mutation)> callback) const {
  auto it = db_->current_transaction()->NewIterator();
  const std::string user_key = LevelDbDocumentOverlayKey::KeyPrefix(user_id_);

  for (it->Seek(user_key); it->Valid() && absl::StartsWith(it->key(), user_key);
       it->Next()) {
    LevelDbDocumentOverlayKey decoded_key;
    HARD_ASSERT(decoded_key.Decode(it->key()));
    callback(it->key(), decoded_key, it->value());
  }
}

}  // namespace local
}  // namespace firestore
}  // namespace firebase
