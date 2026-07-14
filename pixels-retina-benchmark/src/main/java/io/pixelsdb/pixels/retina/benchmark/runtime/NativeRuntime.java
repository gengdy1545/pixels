/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
package io.pixelsdb.pixels.retina.benchmark.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Extracts the real Retina JNI runtime bundled in the executable JAR. */
public final class NativeRuntime
{
    private static volatile boolean initialized;

    private NativeRuntime()
    {
    }

    /**
     * Prepare {@code RGVisibility}'s native dependency before that class is
     * initialized.  A user-supplied {@code pixels.retina.native.library}
     * always wins; otherwise the Linux libraries produced by the repository's
     * own CMake build are extracted from the fat JAR.
     */
    public static synchronized void prepareRetinaLibrary() throws IOException
    {
        if (initialized || System.getProperty("pixels.retina.native.library") != null)
        {
            initialized = true;
            return;
        }
        String os = System.getProperty("os.name", "").toLowerCase();
        if (!os.contains("linux"))
        {
            throw new IOException("the bundled Retina JNI library can only run on Linux; os.name=" + os);
        }

        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome != null && !pixelsHome.isEmpty())
        {
            Path installed = java.nio.file.Paths.get(pixelsHome, "lib", "libpixels-retina.so");
            if (Files.isRegularFile(installed))
            {
                System.setProperty("pixels.retina.native.library", installed.toAbsolutePath().toString());
                initialized = true;
                return;
            }
        }

        Path directory = Files.createTempDirectory("pixels-retina-native-");
        Path jemalloc = extract("/native/linux/libjemalloc.so.2", directory);
        Path retina = extract("/native/linux/libpixels-retina.so", directory);
        directory.toFile().deleteOnExit();
        jemalloc.toFile().deleteOnExit();
        retina.toFile().deleteOnExit();

        /* The repository CMake build gives libpixels-retina.so an $ORIGIN
         * rpath, so its DT_NEEDED jemalloc SONAME resolves to this sibling. */
        System.setProperty("pixels.retina.native.library", retina.toAbsolutePath().toString());
        initialized = true;
    }

    private static Path extract(String resource, Path directory) throws IOException
    {
        String name = resource.substring(resource.lastIndexOf('/') + 1);
        Path target = directory.resolve(name);
        try (InputStream input = NativeRuntime.class.getResourceAsStream(resource))
        {
            if (input == null)
            {
                throw new IOException("native resource is missing from the JAR: " + resource);
            }
            Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }
}
