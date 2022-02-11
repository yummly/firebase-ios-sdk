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
  ForEachKeyWithLargestBatchId(batch_id,
                               [&](absl::string_view encoded_key,
                                   LevelDbDocumentOverlayKey&& decoded_key) {
                                 DeleteOverlay(encoded_key, decoded_key);
                               });
}

DocumentOverlayCache::OverlayByDocumentKeyMap
LevelDbDocumentOverlayCache::GetOverlays(const ResourcePath& collection,
                                         int since_batch_id) const {
  OverlayByDocumentKeyMap result;
  ForEachKeyInCollection(collection, since_batch_id,
                         [&](absl::string_view encoded_key,
                             LevelDbDocumentOverlayKey&& decoded_key) {
                           auto overlay = GetOverlay(encoded_key, decoded_key);
                           HARD_ASSERT(overlay);
                           result[std::move(decoded_key).document_key()] =
                               std::move(overlay).value();
                         });
  return result;
}

DocumentOverlayCache::OverlayByDocumentKeyMap
LevelDbDocumentOverlayCache::GetOverlays(absl::string_view collection_group,
                                         int since_batch_id,
                                         std::size_t count) const {
  absl::optional<int> current_batch_id;
  OverlayByDocumentKeyMap result;
  ForEachKeyInCollectionGroup(
      collection_group, since_batch_id,
      [&](absl::string_view encoded_key,
          LevelDbDocumentOverlayKey&& decoded_key) -> bool {
        if (!current_batch_id) {
          current_batch_id = decoded_key.largest_batch_id();
        } else if (current_batch_id.value() != decoded_key.largest_batch_id()) {
          if (result.size() >= count) {
            return false;
          }
          current_batch_id = decoded_key.largest_batch_id();
        }

        auto overlay = GetOverlay(encoded_key, decoded_key);
        HARD_ASSERT(overlay);
        result[std::move(decoded_key).document_key()] =
            std::move(overlay).value();
        return true;
      });
  return result;
}

int LevelDbDocumentOverlayCache::GetOverlayCount() const {
  return CountEntriesWithKeyPrefix(
      LevelDbDocumentOverlayKey::KeyPrefix(user_id_));
}

int LevelDbDocumentOverlayCache::GetLargestBatchIdIndexEntryCount() const {
  return CountEntriesWithKeyPrefix(
      LevelDbDocumentOverlayLargestBatchIdIndexKey::KeyPrefix(user_id_));
}

int LevelDbDocumentOverlayCache::GetCollectionIndexEntryCount() const {
  return CountEntriesWithKeyPrefix(
      LevelDbDocumentOverlayCollectionIndexKey::KeyPrefix(user_id_));
}

int LevelDbDocumentOverlayCache::GetCollectionGroupIndexEntryCount() const {
  return CountEntriesWithKeyPrefix(
      LevelDbDocumentOverlayCollectionGroupIndexKey::KeyPrefix(user_id_));
}

int LevelDbDocumentOverlayCache::CountEntriesWithKeyPrefix(
    const std::string& key_prefix) const {
  int count = 0;
  auto it = db_->current_transaction()->NewIterator();
  for (it->Seek(key_prefix);
       it->Valid() && absl::StartsWith(it->key(), key_prefix); it->Next()) {
    ++count;
  }
  return count;
}

absl::optional<Overlay> LevelDbDocumentOverlayCache::GetOverlay(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) const {
  auto it = db_->current_transaction()->NewIterator();
  it->Seek(std::string(encoded_key));
  if (!(it->Valid() && it->key() == encoded_key)) {
    return absl::nullopt;
  }
  return ParseOverlay(decoded_key, it->value());
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
  // Remove the existing overlay for the given document key, if it exists.
  DeleteOverlay(key);

  // Calculate the LevelDb key for the new database entry.
  std::string encoded_key =
      LevelDbDocumentOverlayKey::Key(user_id_, key, largest_batch_id);
  LevelDbDocumentOverlayKey decoded_key;
  HARD_ASSERT(decoded_key.Decode(encoded_key));

  // Add index entries for the new database entry.
  PutLargestBatchIdIndexEntryFor(encoded_key, decoded_key);
  PutCollectionIndexEntryFor(encoded_key, decoded_key);
  PutCollectionGroupIndexEntryFor(encoded_key, decoded_key);

  // Put the overlay for the given document key into the database.
  db_->current_transaction()->Put(std::move(encoded_key),
                                  serializer_->EncodeMutation(mutation));
}

void LevelDbDocumentOverlayCache::DeleteOverlay(const model::DocumentKey& key) {
  const std::string leveldb_key_prefix =
      LevelDbDocumentOverlayKey::KeyPrefix(user_id_, key);
  auto it = db_->current_transaction()->NewIterator();
  it->Seek(leveldb_key_prefix);

  if (!(it->Valid() && absl::StartsWith(it->key(), leveldb_key_prefix))) {
    return;
  }

  LevelDbDocumentOverlayKey decoded_key;
  HARD_ASSERT(decoded_key.Decode(it->key()));
  if (decoded_key.document_key() != key) {
    return;
  }

  DeleteOverlay(it->key(), decoded_key);
}

void LevelDbDocumentOverlayCache::DeleteOverlay(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  db_->current_transaction()->Delete(encoded_key);
  DeleteLargestBatchIdIndexEntryFor(encoded_key, decoded_key);
  DeleteCollectionIndexEntryFor(encoded_key, decoded_key);
  DeleteCollectionGroupIndexEntryFor(encoded_key, decoded_key);
}

void LevelDbDocumentOverlayCache::DeleteLargestBatchIdIndexEntryFor(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  db_->current_transaction()->Delete(
      LevelDbDocumentOverlayLargestBatchIdIndexKey::Key(
          user_id_, decoded_key.largest_batch_id(), encoded_key));
}

void LevelDbDocumentOverlayCache::DeleteCollectionIndexEntryFor(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  db_->current_transaction()->Delete(
      LevelDbDocumentOverlayCollectionIndexKey::Key(
          user_id_, decoded_key.document_key().path().PopLast(),
          decoded_key.largest_batch_id(), encoded_key));
}

void LevelDbDocumentOverlayCache::DeleteCollectionGroupIndexEntryFor(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  auto collection_group_opt = decoded_key.document_key().GetCollectionId();
  if (collection_group_opt) {
    db_->current_transaction()->Delete(
        LevelDbDocumentOverlayCollectionGroupIndexKey::Key(
            user_id_, collection_group_opt.value(),
            decoded_key.largest_batch_id(), encoded_key));
  }
}

void LevelDbDocumentOverlayCache::PutLargestBatchIdIndexEntryFor(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  db_->current_transaction()->Put(
      LevelDbDocumentOverlayLargestBatchIdIndexKey::Key(
          user_id_, decoded_key.largest_batch_id(), encoded_key),
      "");
}

void LevelDbDocumentOverlayCache::PutCollectionIndexEntryFor(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  db_->current_transaction()->Put(
      LevelDbDocumentOverlayCollectionIndexKey::Key(
          user_id_, decoded_key.document_key().path().PopLast(),
          decoded_key.largest_batch_id(), encoded_key),
      "");
}

void LevelDbDocumentOverlayCache::PutCollectionGroupIndexEntryFor(
    absl::string_view encoded_key,
    const LevelDbDocumentOverlayKey& decoded_key) {
  auto collection_group_opt = decoded_key.document_key().GetCollectionId();
  if (collection_group_opt) {
    db_->current_transaction()->Put(
        LevelDbDocumentOverlayCollectionGroupIndexKey::Key(
            user_id_, collection_group_opt.value(),
            decoded_key.largest_batch_id(), encoded_key),
        "");
  }
}

void LevelDbDocumentOverlayCache::ForEachKeyWithLargestBatchId(
    int largest_batch_id,
    std::function<void(absl::string_view encoded_key,
                       LevelDbDocumentOverlayKey&& decoded_key)> callback)
    const {
  const std::string key_prefix =
      LevelDbDocumentOverlayLargestBatchIdIndexKey::KeyPrefix(user_id_,
                                                              largest_batch_id);
  auto it = db_->current_transaction()->NewIterator();
  for (it->Seek(key_prefix);
       it->Valid() && absl::StartsWith(it->key(), key_prefix); it->Next()) {
    LevelDbDocumentOverlayLargestBatchIdIndexKey decoded_index_key;
    HARD_ASSERT(decoded_index_key.Decode(it->key()));
    const std::string& encoded_key = decoded_index_key.document_overlays_key();
    LevelDbDocumentOverlayKey decoded_key;
    HARD_ASSERT(decoded_key.Decode(encoded_key));
    callback(encoded_key, std::move(decoded_key));
  }
}

void LevelDbDocumentOverlayCache::ForEachKeyInCollection(
    const ResourcePath& collection,
    int since_batch_id,
    std::function<void(absl::string_view encoded_key,
                       LevelDbDocumentOverlayKey&& decoded_key)> callback)
    const {
  const std::string index_start_key =
      LevelDbDocumentOverlayCollectionIndexKey::KeyPrefix(user_id_, collection,
                                                          since_batch_id + 1);
  const std::string index_key_prefix =
      LevelDbDocumentOverlayCollectionIndexKey::KeyPrefix(user_id_, collection);

  auto it = db_->current_transaction()->NewIterator();
  for (it->Seek(index_start_key);
       it->Valid() && absl::StartsWith(it->key(), index_key_prefix);
       it->Next()) {
    LevelDbDocumentOverlayCollectionIndexKey leveldb_index_key;
    HARD_ASSERT(leveldb_index_key.Decode(it->key()));

    const std::string encoded_key = leveldb_index_key.document_overlays_key();
    LevelDbDocumentOverlayKey decoded_key;
    HARD_ASSERT(decoded_key.Decode(encoded_key));
    if (decoded_key.document_key().path().PopLast() != collection) {
      break;
    }

    callback(encoded_key, std::move(decoded_key));
  }
}

void LevelDbDocumentOverlayCache::ForEachKeyInCollectionGroup(
    absl::string_view collection_group,
    int since_batch_id,
    std::function<bool(absl::string_view encoded_key,
                       LevelDbDocumentOverlayKey&& decoded_key)> callback)
    const {
  const std::string index_start_key =
      LevelDbDocumentOverlayCollectionGroupIndexKey::KeyPrefix(
          user_id_, collection_group, since_batch_id + 1);
  const std::string index_key_prefix =
      LevelDbDocumentOverlayCollectionGroupIndexKey::KeyPrefix(
          user_id_, collection_group);

  auto it = db_->current_transaction()->NewIterator();
  for (it->Seek(index_start_key);
       it->Valid() && absl::StartsWith(it->key(), index_key_prefix);
       it->Next()) {
    LevelDbDocumentOverlayCollectionGroupIndexKey leveldb_index_key;
    HARD_ASSERT(leveldb_index_key.Decode(it->key()));

    const std::string encoded_key = leveldb_index_key.document_overlays_key();
    LevelDbDocumentOverlayKey decoded_key;
    HARD_ASSERT(decoded_key.Decode(encoded_key));
    bool keep_going = callback(encoded_key, std::move(decoded_key));
    if (!keep_going) {
      break;
    }
  }
}

}  // namespace local
}  // namespace firestore
}  // namespace firebase
