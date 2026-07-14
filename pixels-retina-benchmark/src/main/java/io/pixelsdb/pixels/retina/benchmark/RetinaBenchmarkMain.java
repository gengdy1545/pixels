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
package io.pixelsdb.pixels.retina.benchmark;

import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkRunner;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.ResultPrinter;
import io.pixelsdb.pixels.retina.benchmark.index.IndexBenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.index.SnapshotIndexBenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.server.MetadataOnlyMain;
import io.pixelsdb.pixels.retina.benchmark.server.TransactionOnlyMain;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotExporter;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotValidator;
import io.pixelsdb.pixels.retina.benchmark.transaction.TransactionBeginBenchmark;
import io.pixelsdb.pixels.retina.benchmark.transaction.TransactionCommitBenchmark;
import io.pixelsdb.pixels.retina.benchmark.visibility.SnapshotVisibilityBenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.visibility.VisibilityBenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.writebuffer.WriteBufferAddBenchmarkScenario;

import java.util.Arrays;

/** Executable entry point for all independent Retina update benchmarks. */
public final class RetinaBenchmarkMain
{
    private RetinaBenchmarkMain()
    {
    }

    public static void main(String[] args) throws Exception
    {
        if (System.getProperty("log4j.configurationFile") == null)
        {
            System.setProperty("log4j.configurationFile", "retina-benchmark-log4j2.xml");
        }
        if (args.length == 0 || "help".equals(args[0]) || "--help".equals(args[0]))
        {
            printUsage();
            return;
        }

        String command = args[0];
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
        if ("transaction-server".equals(command))
        {
            TransactionOnlyMain.main(commandArgs);
            return;
        }
        if ("metadata-server".equals(command))
        {
            MetadataOnlyMain.main(commandArgs);
            return;
        }
        BenchmarkConfig config = BenchmarkConfig.parse(commandArgs);
        if ("snapshot-export".equals(command))
        {
            SnapshotExporter.run(config);
            return;
        }
        if ("snapshot-validate".equals(command))
        {
            SnapshotValidator.run(config);
            return;
        }

        BenchmarkScenario scenario;
        switch (command)
        {
            case "transaction-begin":
                scenario = new TransactionBeginBenchmark();
                break;
            case "index":
                scenario = config.options().containsKey("snapshot-dir")
                        ? new SnapshotIndexBenchmarkScenario() : new IndexBenchmarkScenario();
                break;
            case "visibility":
                scenario = config.options().containsKey("snapshot-dir")
                        ? new SnapshotVisibilityBenchmarkScenario() : new VisibilityBenchmarkScenario();
                break;
            case "write-buffer-add":
                scenario = new WriteBufferAddBenchmarkScenario();
                break;
            case "transaction-commit":
                scenario = new TransactionCommitBenchmark();
                break;
            default:
                throw new IllegalArgumentException("unknown command: " + command
                        + "; run with --help for available commands");
        }

        BenchmarkResult result = BenchmarkRunner.run(scenario, config);
        ResultPrinter.print(result, System.out);
    }

    private static void printUsage()
    {
        System.out.println("Pixels Retina independent throughput benchmarks");
        System.out.println();
        System.out.println("Usage: java -jar pixels-retina-benchmark-full.jar <command> [options]");
        System.out.println();
        System.out.println("Benchmark commands:");
        System.out.println("  transaction-begin   real TransService BeginTrans/BeginTransBatch RPC");
        System.out.println("  index               local primary update through LocalIndexService");
        System.out.println("  visibility          local ResourceManager -> JNI visibility deletion");
        System.out.println("  write-buffer-add    local PixelsWriteBuffer.addRow");
        System.out.println("  transaction-commit  real TransService CommitTrans/CommitTransBatch RPC");
        System.out.println();
        System.out.println("Snapshot commands:");
        System.out.println("  snapshot-export     export metadata/file topology, real samples, and optional quiesced state");
        System.out.println("  snapshot-validate   verify format, sample payloads/counts, and SHA-256 before restore");
        System.out.println();
        System.out.println("Minimal real service commands:");
        System.out.println("  transaction-server  production TransServer only (requires etcd)");
        System.out.println("  metadata-server     production MetadataServer + NodeServer (requires MySQL + etcd)");
        System.out.println();
        System.out.println("Common options:");
        System.out.println("  --threads N             fixed producer thread count (default: CPU count)");
        System.out.println("  --clients N             RPC channels, write buffers, or local generator labels (default: 1)");
        System.out.println("  --warmup-seconds N      warmup time limit (default: 10; 0 disables)");
        System.out.println("  --duration-seconds N    measured soft time limit (default: 30)");
        System.out.println("  --data-size N           maximum measured logical operations (default: 1000000)");
        System.out.println("  --warmup-operations N   maximum warmup logical operations (default: min(data-size,100000))");
        System.out.println("  --batch-size N          real RPC/index batch, or scalar driver group (default: 1)");
        System.out.println("  --fail-fast true|false  stop after the first failed group (default: false)");
        System.out.println("  --rpc-deadline-ms N     per-call Begin/Commit gRPC deadline (default: 30000)");
        System.out.println("  --snapshot-dir DIR      restore Index/Visibility/WriteBuffer input from a validated snapshot");
        System.out.println("  --snapshot-table NAME   target table (required by Index/Visibility and multi-table WriteBuffer snapshots)");
        System.out.println("  --snapshot-require LIST validation profiles: index, visibility, visibility-clean, write-buffer");
        System.out.println();
        System.out.println("Scenario options are in README.md; snapshot export/restore is in SNAPSHOT.md.");
    }
}
