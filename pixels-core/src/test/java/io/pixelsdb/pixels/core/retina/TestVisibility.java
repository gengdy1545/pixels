/*
 * Copyright 2024 PixelsDB.
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

package io.pixelsdb.pixels.core.retina;

public class TestVisibility
{
    public static void main(String[] args) {
        try (Visibility tracker = new Visibility()) {
            // Test getReadableBitmap method
            long[] bitmap = tracker.getReadableBitmap(12345);
            System.out.println("Readable Bitmap: " + java.util.Arrays.toString(bitmap));

            // Test deleteRow method
            tracker.deleteRow(12345, 1);
            System.out.println("Row 1 deleted at timestamp 12345");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}