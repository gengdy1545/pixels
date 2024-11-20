//
// Created by whz on 11/19/24.
//

#ifndef INTEGER128_H
#define INTEGER128_H

#include <cstdint>
#include <array>
#include <stdexcept>
#include <string>
#include <ostream>
#include <vector>

class Integer128 {
public:
  static const Integer128 MAX_VALUE;
  static const Integer128 MIN_VALUE;
  static const Integer128 ONE;
  static const Integer128 ZERO;

  Integer128(int64_t high, uint64_t low);

  void update(int64_t high, uint64_t low);

  int64_t getHigh() const;
  uint64_t getLow() const;

  static Integer128 fromBigEndian(const std::vector<uint8_t>& bytes);

  static Integer128 valueOf(int64_t high, uint64_t low);
  static Integer128 valueOf(int64_t value);
  static Integer128 valueOf(const std::string& value);

  void add(int64_t high, uint64_t low);
  bool equals(const Integer128& other) const;

  bool operator==(const Integer128& other) const;
  bool operator!=(const Integer128& other) const;
  bool operator<(const Integer128& other) const;
  bool operator<=(const Integer128& other) const;
  bool operator>(const Integer128& other) const;
  bool operator>=(const Integer128& other) const;

  std::string toString() const;
  bool isZero() const;
  bool isNegative() const;

  std::array<uint8_t, 16> toBigEndianBytes() const;
  static std::array<uint8_t, 8> toBigEndianBytes(uint64_t value);
  static uint64_t fromBigEndianBytes(const std::vector<uint8_t>& bytes, size_t offset);

  friend std::ostream& operator<<(std::ostream& os, const Integer128& value);

private:
  int64_t high;
  uint64_t low;
};

#endif // INTEGER128_H

