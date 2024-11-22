//
// Created by whz on 11/19/24.
//

#ifndef CPP_ENCODINGLEVEL_H
#define CPP_ENCODINGLEVEL_H

#include <stdexcept>
#include <string>
#include <iostream>
#include <cassert>
#include <memory>

class EncodingLevel {
public:

    enum class Level {
        EL0 = 0,
        EL1 = 1,
        EL2 = 2
    };

    explicit EncodingLevel(Level level) : level(level) {}

    static EncodingLevel from(int level) {
        switch (level) {
            case 0: return EncodingLevel(Level::EL0);
            case 1: return EncodingLevel(Level::EL1);
            case 2: return EncodingLevel(Level::EL2);
            default: throw std::invalid_argument("Invalid encoding level: " + std::to_string(level));
        }
    }

    static EncodingLevel from(const std::string& levelStr) {
        if (levelStr.empty()) {
            throw std::invalid_argument("level is null");
        }
        return from(std::stoi(levelStr));
    }

    static bool isValid(int level) {
        return level >= 0 && level <= 2;
    }

    bool ge(int otherLevel) const {
        if (!isValid(otherLevel)) {
            throw std::invalid_argument("Level is invalid");
        }
        return static_cast<int>(level) >= otherLevel;
    }

    bool ge(Level otherLevel) const {
        return static_cast<int>(level) >= static_cast<int>(otherLevel);
    }

    bool ge(const EncodingLevel& other) const {
        return static_cast<int>(level) >= static_cast<int>(other.level);
    }

    bool equals(int otherLevel) const {
        return static_cast<int>(level) == otherLevel;
    }

    bool equals(const EncodingLevel& other) const {
        return level == other.level;
    }

    Level getLevel() const {
        return level;
    }


    int toInt() const {
        return static_cast<int>(level);
    }
private:
    Level level;
};



#endif //CPP_ENCODINGLEVEL_H
