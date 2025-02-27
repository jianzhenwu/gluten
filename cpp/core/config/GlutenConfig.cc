/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jni.h>

#include "compute/ProtobufUtils.h"
#include "config.pb.h"
#include "jni/JniError.h"

namespace gluten {
std::unordered_map<std::string, std::string> parseConfMap(JNIEnv* env, jbyteArray configArray) {
  std::unordered_map<std::string, std::string> sparkConfs;
  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(configArray, 0));
  auto planSize = env->GetArrayLength(configArray);
  ConfigMap pConfigMap;
  gluten::parseProtobuf(planData, planSize, &pConfigMap);
  for (const auto& pair : pConfigMap.configs()) {
    sparkConfs.emplace(pair.first, pair.second);
  }

  return sparkConfs;
}

std::string printConfig(const std::unordered_map<std::string, std::string>& conf) {
  std::ostringstream oss;
  oss << std::endl;
  for (auto& [k, v] : conf) {
    oss << " [" << k << ", " << v << "]\n";
  }
  return oss.str();
}
} // namespace gluten
