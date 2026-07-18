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

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkRunner;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.ResultPrinter;
import io.pixelsdb.pixels.retina.benchmark.index.IndexOperation;
import io.pixelsdb.pixels.retina.benchmark.index.PhysicalIndexBenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.server.MetadataOnlyMain;
import io.pixelsdb.pixels.retina.benchmark.server.TransactionOnlyMain;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotExporter;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotValidator;
import io.pixelsdb.pixels.retina.benchmark.transaction.TransactionBeginBenchmark;
import io.pixelsdb.pixels.retina.benchmark.transaction.TransactionBeginCommitBenchmark;
import io.pixelsdb.pixels.retina.benchmark.transaction.TransactionCommitBenchmark;
import io.pixelsdb.pixels.retina.benchmark.visibility.SnapshotVisibilityBenchmarkScenario;
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
        if ("metadata-drop-schema".equals(command))
        {
            String schema = config.require("schema");
            MetadataService metadata = MetadataService.Instance();
            boolean existed = metadata.existSchema(schema);
            if (existed)
            {
                metadata.dropSchema(schema);
            }
            System.out.println("schema=" + schema);
            System.out.println("existed=" + existed);
            System.out.println("status=dropped");
            System.out.flush();
            Runtime.getRuntime().halt(0);
            return;
        }
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
            case "transaction-begin-commit":
                scenario = new TransactionBeginCommitBenchmark();
                break;
            case "transaction-commit":
                scenario = new TransactionCommitBenchmark();
                break;
            case "index-put-primary":
                scenario = new PhysicalIndexBenchmarkScenario(IndexOperation.PUT_PRIMARY);
                break;
            case "index-update-primary":
                scenario = new PhysicalIndexBenchmarkScenario(IndexOperation.UPDATE_PRIMARY);
                break;
            case "index-delete-primary":
                scenario = new PhysicalIndexBenchmarkScenario(IndexOperation.DELETE_PRIMARY);
                break;
            case "index-put-secondary":
                scenario = new PhysicalIndexBenchmarkScenario(IndexOperation.PUT_SECONDARY);
                break;
            case "index-update-secondary":
                scenario = new PhysicalIndexBenchmarkScenario(IndexOperation.UPDATE_SECONDARY);
                break;
            case "index-delete-secondary":
                scenario = new PhysicalIndexBenchmarkScenario(IndexOperation.DELETE_SECONDARY);
                break;
            case "visibility":
                scenario = new SnapshotVisibilityBenchmarkScenario();
                break;
            case "write-buffer-add":
                scenario = new WriteBufferAddBenchmarkScenario();
                break;
            default:
                throw new IllegalArgumentException("unknown command: " + command
                        + "; run with --help for available commands");
        }

        BenchmarkResult result = BenchmarkRunner.run(scenario, config);
        ResultPrinter.print(result, System.out);
        System.out.flush();
        if ("write-buffer-add".equals(command)
                && config.getBoolean("writebuffer-skip-measurement-close", false))
        {
            Runtime.getRuntime().halt(0);
        }
    }

    private static void printUsage()
    {
        System.out.println("Pixels Retina independent throughput benchmarks");
        System.out.println();
        System.out.println("Usage: java -jar pixels-retina-benchmark-full.jar <command> [options]");
        System.out.println();
        System.out.println("Benchmark commands:");
        System.out.println("  transaction-begin         real TransService BeginTrans/BeginTransBatch RPC");
        System.out.println("  transaction-begin-commit  real Begin followed immediately by Commit");
        System.out.println("  transaction-commit        real TransService CommitTrans/CommitTransBatch RPC");
        System.out.println("  index-put-primary         LocalIndexService.putPrimaryIndexEntries");
        System.out.println("  index-update-primary      LocalIndexService.updatePrimaryIndexEntries");
        System.out.println("  index-delete-primary      LocalIndexService.deletePrimaryIndexEntries");
        System.out.println("  index-put-secondary       LocalIndexService.putSecondaryIndexEntries");
        System.out.println("  index-update-secondary    LocalIndexService.updateSecondaryIndexEntries");
        System.out.println("  index-delete-secondary    LocalIndexService.deleteSecondaryIndexEntries");
        System.out.println("  visibility                local ResourceManager -> JNI visibility deletion");
        System.out.println("  write-buffer-add          local PixelsWriteBuffer.addRow");
        System.out.println();
        System.out.println("Snapshot commands:");
        System.out.println("  snapshot-export     export v2 topology and optional quiesced physical state");
        System.out.println("  snapshot-validate   verify v2 semantics, profiles, and SHA-256 artifacts");
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
        System.out.println("  --snapshot-table NAME   target table");
        System.out.println("  --snapshot-secondary-index ID|NAME  target secondary index for secondary commands");
        System.out.println("  --snapshot-require LIST validation profiles: index, visibility, write-buffer");
        System.out.println();
        System.out.println("Scenario options are in README.md; snapshot export/restore is in SNAPSHOT.md.");
    }
}
