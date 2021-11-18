/*
 * Copyright 2021 Google LLC
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
#ifndef FIRESTORE_CORE_SRC_REMOTE_GRPC_ADAPT_GRPC_SWIFT_MISC_H_
#define FIRESTORE_CORE_SRC_REMOTE_GRPC_ADAPT_GRPC_SWIFT_MISC_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "Firestore/core/src/remote/grpc_adapt/grpc_swift_channel.h"
#include "Firestore/core/src/remote/grpc_adapt/grpc_swift_status.h"
#include "Firestore/core/src/remote/grpc_adapt/grpc_swift_string_ref.h"

namespace firebase {
namespace firestore {
namespace remote {
namespace grpc_adapt {

class Slice {
 public:
  Slice(const void* buf, size_t len);
  Slice(const std::string& s);
  /// Byte size.
  size_t size() const;

  /// Raw pointer to the beginning (first element) of the slice.
  const uint8_t* begin() const;
};

class ByteBuffer final {
 public:
  /// Constuct an empty buffer.
  ByteBuffer();
  ByteBuffer(const Slice* slices, size_t nslices);

  /// Buffer size in bytes.
  size_t Length() const;
  /// Dump (read) the buffer contents into \a slices.
  Status Dump(std::vector<Slice>* slices) const;
};

class GrpcLibraryCodegen {};

static std::string Version() {
  return "";
}

}  // namespace grpc_adapt
}  // namespace remote
}  // namespace firestore
}  // namespace firebase

#endif  // FIRESTORE_CORE_SRC_REMOTE_GRPC_ADAPT_GRPC_SWIFT_MISC_H_
