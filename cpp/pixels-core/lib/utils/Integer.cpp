//
// Created by whz on 11/19/24.
//

#include "utils/Integer.h"
#include <vector>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <cmath>
#include <cstring>

const Integer128 Integer128::MAX_VALUE = Integer128(0x7FFF'FFFF'FFFF'FFFFL, 0xFFFF'FFFF'FFFF'FFFFUL);
const Integer128 Integer128::MIN_VALUE = Integer128(0x8000'0000'0000'0000L, 0x0000'0000'0000'0000UL);
const Integer128 Integer128::ONE = Integer128(0, 1);
const Integer128 Integer128::ZERO = Integer128(0, 0);

Integer128::Integer128(int64_t high, uint64_t low) : high(high), low(low) {}

void Integer128::update(int64_t high, uint64_t low) {
  this->high = high;
  this->low = low;
}

int64_t Integer128::getHigh() const {
  return high;
}

uint64_t Integer128::getLow() const {
  return low;
}

Integer128 Integer128::fromBigEndian(const std::vector<uint8_t>& bytes) {
  if (bytes.empty()) {
    throw std::invalid_argument("Empty byte array");
  }

  size_t size = bytes.size();
  uint64_t low = fromBigEndianBytes(bytes, size >= 8 ? size - 8 : 0);
  int64_t high = 0;

  if (size > 8) {
    high = static_cast<int64_t>(fromBigEndianBytes(bytes, size - 16));
  } else if (size < 8) {
    for (uint8_t byte : bytes) {
      high = (high << 8) | byte;
    }
    high = (high >> 63);
  }

  return Integer128(high, low);
}

Integer128 Integer128::valueOf(int64_t high, uint64_t low) {
  return Integer128(high, low);
}

Integer128 Integer128::valueOf(int64_t value) {
  return Integer128(value < 0 ? -1 : 0, static_cast<uint64_t>(value));
}

std::array<uint8_t, 16> Integer128::toBigEndianBytes() const {
  std::array<uint8_t, 16> result{};
  auto highBytes = toBigEndianBytes(static_cast<uint64_t>(high));
  auto lowBytes = toBigEndianBytes(low);
  std::copy(highBytes.begin(), highBytes.end(), result.begin());
  std::copy(lowBytes.begin(), lowBytes.end(), result.begin() + 8);
  return result;
}

std::array<uint8_t, 8> Integer128::toBigEndianBytes(uint64_t value) {
  std::array<uint8_t, 8> result{};
  for (int i = 7; i >= 0; --i) {
    result[i] = static_cast<uint8_t>(value & 0xFF);
    value >>= 8;
  }
  return result;
}

uint64_t Integer128::fromBigEndianBytes(const std::vector<uint8_t>& bytes, size_t offset) {
  uint64_t value = 0;
  size_t end = offset + 8;
  for (size_t i = offset; i < end; ++i) {
    value = (value << 8) | bytes[i];
  }
  return value;
}

void Integer128::add(int64_t high, uint64_t low) {
  uint64_t newLow = this->low + low;
  int64_t carry = (newLow < this->low) ? 1 : 0;
  this->low = newLow;
  this->high += high + carry;
}

bool Integer128::equals(const Integer128& other) const {
  return high == other.high && low == other.low;
}

bool Integer128::operator==(const Integer128& other) const {
  return equals(other);
}

bool Integer128::operator!=(const Integer128& other) const {
  return !equals(other);
}

bool Integer128::operator<(const Integer128& other) const {
  return high < other.high || (high == other.high && low < other.low);
}

bool Integer128::operator<=(const Integer128& other) const {

