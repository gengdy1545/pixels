/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.plan.physical.output;

import io.pixelsdb.pixels.common.turbo.Output;

import java.util.ArrayList;

/**
 * The output format for serverless operators.
 * @author hank
 * @create 2022-04-11
 */
public abstract class NonPartitionOutput extends Output
{
    /**
     * The number of row groups in each result files.
     */
    private ArrayList<Integer> rowGroupNums = new ArrayList<>();

    /**
     * Default constructor for jackson.
     */
    public NonPartitionOutput() { }

    public ArrayList<Integer> getRowGroupNums()
    {
        return rowGroupNums;
    }

    public void setRowGroupNums(ArrayList<Integer> rowGroupNums)
    {
        this.rowGroupNums = rowGroupNums;
    }

    public synchronized void addOutput(String output, int rowGroupNum)
    {
        this.addOutput(output);
        this.rowGroupNums.add(rowGroupNum);
    }
}
