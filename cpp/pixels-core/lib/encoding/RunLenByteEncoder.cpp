#include "encoding/RunLenByteEncoder.h"

void RunLenByteEncoder::write(uint8_t value)
{
    if (numLiterals == 0)
    {
        literals[numLiterals++] = value;
        tailRunLength = 1;
    }
    else if (repeat)
    {
        if (value == literals[0])
        {
            numLiterals += 1;
            if (numLiterals == MAX_REPEAT_SIZE)
            {
                writeValues();
            }
        }
        else
        {
            writeValues();
            literals[numLiterals++] = value;
            tailRunLength = 1;
        }
    }
    else
    {
        if (value == literals[numLiterals - 1])
        {
            tailRunLength += 1;
        }
        else
        {
            tailRunLength = 1;
        }
        if (tailRunLength == MIN_REPEAT_SIZE)
        {
            if (numLiterals + 1 == MIN_REPEAT_SIZE)
            {
                repeat = true;
                numLiterals += 1;
            }
            else
            {
                numLiterals -= MIN_REPEAT_SIZE - 1;
                writeValues();
                literals[0] = value;
                repeat = true;
                numLiterals = MIN_REPEAT_SIZE;
            }
        }
        else
        {
            literals[numLiterals++] = value;
            if (numLiterals == MAX_LITERAL_SIZE)
            {
                writeValues();
            }
        }
    }
}

void RunLenByteEncoder::writeValues()
{
    if (numLiterals != 0)
    {
        if (repeat)
        {
            output->putInt(numLiterals - MIN_REPEAT_SIZE);
            output->putBytes(literals.data(), 1);
        }
        else
        {
            output->putInt(-numLiterals);
            output->putBytes(literals.data(), numLiterals);
        }
        repeat = false;
        tailRunLength = 0;
        numLiterals = 0;
    }
}

void RunLenByteEncoder::flush()
{
    writeValues();
}

RunLenByteEncoder::RunLenByteEncoder()
{
    output = std::make_shared<ByteBuffer>();
}

void RunLenByteEncoder::encode(uint8_t *values, uint8_t *results, int length, int &resultLength)
{
    for (int i = 0; i < length; i++)
    {
        write(values[i]);
    }
    flush();
    resultLength = output->getWritePos();
    output->getBytes(results, resultLength);
    output->clear();
}

void RunLenByteEncoder::close()
{
    output->clear();
    output = nullptr;
}
