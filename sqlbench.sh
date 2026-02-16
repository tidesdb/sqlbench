#!/bin/bash
#                                                    
#                 8 8                          8      
#                 8 8                          8      
#   .oPYo. .oPYo. 8 8oPYo. .oPYo. odYo. .oPYo. 8oPYo. 
#   Yb..   8    8 8 8    8 8oooo8 8' `8 8    ' 8    8 
#     'Yb. 8    8 8 8    8 8.     8   8 8    . 8    8 
#   `YooP' `YooP8 8 `YooP' `Yooo' 8   8 `YooP' 8    8 
#   :.....::....8 ..:.....::.....:..::..:.....:..:::..
#   ::::::::::::8 ::::::::::::::::::::::::::::::::::::
#   ::::::::::::..::::::::::::::::::::::::::::::::::::
#
#   OLTP benchmark with multiple workloads, table sizes, thread counts,
#   and iterations per configuration.
#   Outputs results to CSV for easy comparison and analysis
#
#   License: GPLv2
#   Copyright (C) 2026 Alex Gaetano Padula
# 
#   ---------------------------------
#   Usage
#   ---------------------------------
#   Make sure you have latest sysbench installed
#   ---------------------------------
#   https://github.com/akopytov/sysbench
#
#   Quick test
#   ---------------------------------
#   ./sqlbench.sh
#
#   Full benchmark
#   ---------------------------------
#   TABLE_SIZES="100000 1000000" \
#   THREAD_COUNTS="1 4 8 16" \
#   TIME=300 \
#   WARMUP=30 \
#   ITERATIONS=3 \
#   ./sqlbench.sh
#
#   Environment variables
#   ---------------------------------
#   DATA_DIR                - Base data directory for MariaDB (will start server automatically)
#   INNODB_DATA_DIR         - Custom InnoDB data/tablespace directory (for fast disk)
#   SOCKET                  - MySQL socket path (default: MTR socket or DATA_DIR/mysqld.sock)
#   TABLE_SIZES             - Space-separated list of table sizes (default: "10000")
#   TABLES                  - Number of sysbench tables (default: 1)
#   THREAD_COUNTS           - Space-separated list of thread counts (default: "1")
#   TIME                    - Benchmark duration in seconds (default: 60)
#   WARMUP                  - Warmup duration in seconds before measurement (default: 10)
#   ENGINES                 - Space-separated list of engines to test (default: "InnoDB TidesDB")
#   ITERATIONS              - Number of iterations per test configuration (default: 1)
#   WORKLOADS               - Space-separated list of workloads (default: all OLTP workloads)
#
#   TidesDB per-table options (applied via CREATE TABLE)
#   ---------------------------------------------
#   TIDESDB_SYNC_MODE       - 0=NONE, 1=INTERVAL, 2=FULL (default: 0 for benchmarking)
#                             Note -- plugin default is FULL(2); script overrides to NONE(0) for perf
#   TIDESDB_USE_BTREE       - B+tree SSTable format (default: 0=OFF, set 1 for ON)
#   TIDESDB_COMPRESSION     - NONE, SNAPPY, LZ4, ZSTD, LZ4_FAST (default: LZ4)
#
#   TidesDB global tuning (server-level, applied at startup)
#   ---------------------------------
#   TIDESDB_FLUSH_THREADS   - Number of flush threads (default: 2)
#   TIDESDB_COMPACT_THREADS - Number of compaction threads (default: 2)
#   TIDESDB_BLOCK_CACHE     - Block cache size in bytes (default: 268435456 = 256MB)
#   TIDESDB_MAX_SSTABLES    - Max open SSTable structures in LRU cache (default: 256)
#
#   InnoDB tuning
#   ---------------------------------
#   INNODB_BUFFER_POOL      - InnoDB buffer pool size (default: 256M)
#   INNODB_FLUSH            - InnoDB flush_log_at_trx_commit value (default: 0 for benchmarking)
#
#   Available workloads
#   ---------------------------------
#   oltp_read_only          - Read-only transactions (point selects + range scans)
#   oltp_write_only         - Write-only transactions (inserts, updates, deletes)
#   oltp_read_write         - Mixed read-write transactions
#   oltp_point_select       - Pure point lookups (tests bloom filters)
#   oltp_insert             - Pure inserts (LSM-tree strength)
#   oltp_update_index       - Updates on indexed columns
#   oltp_update_non_index   - Updates on non-indexed columns
#   oltp_delete             - Delete operations
#   select_random_ranges    - Range scans (tests LSM merge overhead)
#
#   Execution order
#   ---------------------------------
#   for each table_size
#     for each thread_count
#       for each engine
#         for each workload
#           for each iteration (1..ITERATIONS)
#             cleanup leftover tables
#             wipe TidesDB data dir (if stale data present)
#             prepare (create tables, insert rows)
#             measure data size after prepare
#             warmup
#             run benchmark
#             measure data size after run
#             record results to CSV
#             cleanup tables
#
#   Each iteration is a full prepare/warmup/run/cleanup cycle from a clean
#   state, so results across iterations are independently comparable.
#   Use ITERATIONS>1 to compute averages and standard deviations.
#
#   Example with custom I/O directories on fast NVMe
#   ---------------------------------
#   INNODB_DATA_DIR=/mnt/nvme/innodb TIDESDB_DATA_DIR=/mnt/nvme/tidesdb ./sqlbench.sh
#

set +e

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-/home/agpmastersystem/server-mariadb-12.1.2/build}"

# MariaDB client path
MYSQL_BIN="${MYSQL_BIN:-${BUILD_DIR}/client/mariadb}"

# Configuration
DATA_DIR="${DATA_DIR:-}"
if [ -n "$DATA_DIR" ]; then
    SOCKET="${SOCKET:-${DATA_DIR}/mysqld.sock}"
else
    SOCKET="${SOCKET:-${BUILD_DIR}/mysql-test/var/tmp/mysqld.1.sock}"
fi
DB_USER="${MYSQL_USER:-root}"
DB="${MYSQL_DB:-test}"

# Support multiple table sizes and thread counts
TABLE_SIZES="${TABLE_SIZES:-10000}"
TABLES="${TABLES:-1}"
THREAD_COUNTS="${THREAD_COUNTS:-1}"
TIME="${TIME:-60}"
WARMUP="${WARMUP:-10}"
ITERATIONS="${ITERATIONS:-1}"
REPORT_INTERVAL="${REPORT_INTERVAL:-10}"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/results}"

# TidesDB per-table option defaults
TIDESDB_COMPRESSION="${TIDESDB_COMPRESSION:-LZ4}"

# TidesDB global tuning defaults
TIDESDB_FLUSH_THREADS="${TIDESDB_FLUSH_THREADS:-2}"
TIDESDB_COMPACT_THREADS="${TIDESDB_COMPACT_THREADS:-2}"
TIDESDB_BLOCK_CACHE="${TIDESDB_BLOCK_CACHE:-268435456}"

# InnoDB tuning defaults
INNODB_BUFFER_POOL="${INNODB_BUFFER_POOL:-256M}"
TIDESDB_MAX_SSTABLES="${TIDESDB_MAX_SSTABLES:-256}"

# Default workloads -- OLTP coverage
DEFAULT_WORKLOADS="oltp_point_select oltp_read_only oltp_write_only oltp_read_write oltp_insert oltp_update_index oltp_update_non_index oltp_delete"
WORKLOADS="${WORKLOADS:-$DEFAULT_WORKLOADS}"

mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY_CSV="$OUTPUT_DIR/summary_${TIMESTAMP}.csv"
DETAIL_CSV="$OUTPUT_DIR/detail_${TIMESTAMP}.csv"
LATENCY_CSV="$OUTPUT_DIR/latency_${TIMESTAMP}.csv"

# CSV headers
SIZES_CSV="$OUTPUT_DIR/sizes_${TIMESTAMP}.csv"

echo "engine,workload,threads,table_size,iteration,tps,qps,reads_per_sec,writes_per_sec,latency_avg_ms,latency_min_ms,latency_p95_ms,latency_max_ms,errors,reconnects,total_time_s,warmup_s,data_size_after_prepare_mb,data_size_after_run_mb" > "$SUMMARY_CSV"
echo "engine,workload,threads,table_size,iteration,time_s,tps,qps,latency_avg_ms,latency_p95_ms" > "$DETAIL_CSV"
echo "engine,workload,threads,table_size,iteration,percentile,latency_ms" > "$LATENCY_CSV"
echo "engine,workload,threads,table_size,iteration,phase,size_bytes,size_mb" > "$SIZES_CSV"

# Measure engine-specific data directory size.
# For InnoDB    -- .ibd files in DATA_DIR/test/ + system tablespace + redo logs
# For TidesDB   -- everything under TIDESDB_DIR/
measure_data_size() {
    local engine=$1
    local size_bytes=0

    if [ -z "$DATA_DIR" ]; then
        echo "0"
        return
    fi

    if [ "$engine" = "TidesDB" ]; then
        # TidesDB stores all data under TIDESDB_DIR
        if [ -d "$TIDESDB_DIR" ]; then
            size_bytes=$(du -sb "$TIDESDB_DIR" 2>/dev/null | awk '{print $1}')
        fi
    else
        # InnoDB -- .ibd files in the database dir + system tablespace + redo logs
        local ibd_size=0
        if [ -d "$DATA_DIR/test" ]; then
            ibd_size=$(find "$DATA_DIR/test" -name '*.ibd' -exec du -sb {} + 2>/dev/null | awk '{s+=$1} END {print s+0}')
        fi
        local sys_size=0
        if [ -d "$INNODB_DIR" ]; then
            sys_size=$(find "$INNODB_DIR" \( -name 'ibdata*' -o -name 'ib_logfile*' -o -name 'ib_redo*' \) -exec du -sb {} + 2>/dev/null | awk '{s+=$1} END {print s+0}')
        fi
        size_bytes=$(( ${ibd_size:-0} + ${sys_size:-0} ))
    fi

    echo "${size_bytes:-0}"
}

run_sysbench_test() {
    local engine=$1
    local test=$2
    local threads=$3
    local table_size=$4
    local iteration=$5

    # We extract just the test name for display and filenames
    local test_name=$(basename "$test" .lua)

    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  $engine - $test_name (threads=$threads, table_size=$table_size, iter=$iteration/$ITERATIONS)"
    echo "═══════════════════════════════════════════════════════════════════"

    # We cleanup any leftover tables from a previous run
    sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$DB_USER" \
        --mysql-db="$DB" \
        --tables="$TABLES" \
        cleanup > /dev/null 2>&1 || true

    # For TidesDB -- We wipe the data directory between iterations so that
    # size measurements are per-run, not cumulative.  Requires a
    # server restart since TidesDB holds the DB open.
    if [ "$engine" = "TidesDB" ] && [ -n "$DATA_DIR" ] && [ -d "$TIDESDB_DIR" ]; then
        local tdb_size_before_wipe
        tdb_size_before_wipe=$(du -sb "$TIDESDB_DIR" 2>/dev/null | awk '{print $1}')
        if [ "${tdb_size_before_wipe:-0}" -gt 1048576 ]; then
            echo "  [CLEANUP] Wiping TidesDB data dir ($(echo "scale=1; ${tdb_size_before_wipe}/1048576" | bc) MB stale data)..."
            stop_server
            rm -rf "$TIDESDB_DIR"
            mkdir -p "$TIDESDB_DIR"
            if ! start_server; then
                echo "  [ERROR] Failed to restart server after TidesDB wipe"
                return 1
            fi
        fi
    fi

    # Build per-engine CREATE TABLE options.
    # These must be passed via --create_table_options so they are part of the
    # original CREATE TABLE statement.  CF-level config (sync_mode, compression,
    # use_btree, etc.) is baked into the column family at creation time and
    # cannot be changed by a post-CREATE ALTER (which only updates .frm).
    local create_opts=""
    if [ "$engine" = "TidesDB" ]; then
        local sync_val="${TIDESDB_SYNC_MODE:-0}"
        local sync_name="NONE"
        case "$sync_val" in
            0) sync_name="NONE" ;;
            1) sync_name="INTERVAL" ;;
            2) sync_name="FULL" ;;
        esac
        create_opts="SYNC_MODE='${sync_name}'"
        create_opts="${create_opts} COMPRESSION='${TIDESDB_COMPRESSION}'"
        if [ "${TIDESDB_USE_BTREE:-0}" = "1" ]; then
            create_opts="${create_opts} USE_BTREE=1"
        fi
    fi

    # Prepare -- we pass engine options in CREATE TABLE via --create_table_options
    echo "▶ Preparing $TABLES table(s) with $table_size rows (${engine}${create_opts:+ [$create_opts]})..."
    local -a sb_extra_opts=()
    if [ -n "$create_opts" ]; then
        sb_extra_opts+=("--create_table_options=${create_opts}")
    fi
    if ! sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$DB_USER" \
        --mysql-db="$DB" \
        --tables="$TABLES" \
        --table-size="$table_size" \
        --threads=1 \
        --mysql-storage-engine="$engine" \
        "${sb_extra_opts[@]}" \
        prepare 2>&1; then
        echo "  [ERROR] Prepare failed for $engine $test_name"
        return 1
    fi

    # Measure data directory size after prepare
    local size_after_prepare=$(measure_data_size "$engine")
    local size_after_prepare_mb=$(echo "scale=2; ${size_after_prepare} / 1048576" | bc 2>/dev/null || echo "0")
    echo "  [SIZE] Data size after prepare: ${size_after_prepare_mb} MB"

    # Warmup (if configured)
    if [ "$WARMUP" -gt 0 ]; then
        echo "▶ Warming up for ${WARMUP}s..."
        sysbench "$test" \
            --mysql-socket="$SOCKET" \
            --mysql-user="$DB_USER" \
            --mysql-db="$DB" \
            --tables="$TABLES" \
            --table-size="$table_size" \
            --threads="$threads" \
            --time="$WARMUP" \
            --mysql-storage-engine="$engine" \
            --mysql-ignore-errors=1213,1020,1205,1180 \
            run > /dev/null 2>&1 || true
    fi

    echo "▶ Running benchmark for ${TIME}s with $threads threads..."
    local output_file="$OUTPUT_DIR/${engine}_${test_name}_t${threads}_s${table_size}_i${iteration}_${TIMESTAMP}.txt"

    sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$DB_USER" \
        --mysql-db="$DB" \
        --tables="$TABLES" \
        --table-size="$table_size" \
        --threads="$threads" \
        --time="$TIME" \
        --mysql-storage-engine="$engine" \
        --report-interval="$REPORT_INTERVAL" \
        --histogram=on \
        --percentile=95 \
        --mysql-ignore-errors=1213,1020,1205,1180 \
        run 2>&1 | tee "$output_file"

    # We parse results from sysbench summary output
    local tps=$(grep "transactions:" "$output_file" | awk '{print $2}' | sed 's/(//g')
    local qps=$(grep "queries:" "$output_file" | head -1 | awk '{print $2}' | sed 's/(//g')
    local reads=$(grep "read:" "$output_file" | head -1 | awk '{print $2}')
    local writes=$(grep "write:" "$output_file" | head -1 | awk '{print $2}')
    local lat_avg=$(grep "avg:" "$output_file" | tail -1 | awk '{print $2}')
    local lat_min=$(grep "min:" "$output_file" | tail -1 | awk '{print $2}')
    local lat_p95=$(grep "95th percentile:" "$output_file" | awk '{print $3}' || echo "0")
    local lat_max=$(grep "max:" "$output_file" | tail -1 | awk '{print $2}')
    local errors=$(grep "ignored errors:" "$output_file" | awk '{print $3}' || echo "0")
    local reconnects=$(grep "reconnects:" "$output_file" | head -1 | awk '{print $2}' || echo "0")
    local total_time=$(grep "time elapsed:" "$output_file" | awk '{print $3}' | sed 's/s//g')

    # We calculate reads/writes per second
    local reads_per_sec=$(echo "scale=2; ${reads:-0} / ${total_time:-1}" | bc 2>/dev/null || echo "0")
    local writes_per_sec=$(echo "scale=2; ${writes:-0} / ${total_time:-1}" | bc 2>/dev/null || echo "0")

    # Measure data directory size after run (before cleanup)
    local size_after_run=$(measure_data_size "$engine")
    local size_after_run_mb=$(echo "scale=2; ${size_after_run} / 1048576" | bc 2>/dev/null || echo "0")
    echo "  [SIZE] Data size after run: ${size_after_run_mb} MB"

    # We write to summary CSV
    echo "$engine,$test_name,$threads,$table_size,$iteration,$tps,$qps,$reads_per_sec,$writes_per_sec,$lat_avg,$lat_min,$lat_p95,$lat_max,$errors,$reconnects,$total_time,$WARMUP,$size_after_prepare_mb,$size_after_run_mb" >> "$SUMMARY_CSV"

    # Write to sizes CSV
    echo "$engine,$test_name,$threads,$table_size,$iteration,after_prepare,$size_after_prepare,$size_after_prepare_mb" >> "$SIZES_CSV"
    echo "$engine,$test_name,$threads,$table_size,$iteration,after_run,$size_after_run,$size_after_run_mb" >> "$SIZES_CSV"

    # We parse the interval reports for detail CSV
    # Format -- [ 10s ] thds: N tps: X qps: Y ... lat (ms,95%): Z
    grep "thds:" "$output_file" | while IFS= read -r line; do
        local time_s=$(echo "$line" | sed -n 's/.*\[ *\([0-9.]*\)s \].*/\1/p')
        local int_tps=$(echo "$line" | sed -n 's/.*tps: *\([0-9.]*\).*/\1/p')
        local int_qps=$(echo "$line" | sed -n 's/.*qps: *\([0-9.]*\).*/\1/p')
        local int_lat=$(echo "$line" | sed -n 's/.*lat (ms,[0-9]*%): *\([0-9.]*\).*/\1/p')
        local int_p95="$int_lat"
        echo "$engine,$test_name,$threads,$table_size,$iteration,$time_s,$int_tps,$int_qps,$int_lat,$int_p95" >> "$DETAIL_CSV"
    done

    # We parse histogram for latency distribution CSV
    if grep -q "Latency histogram" "$output_file"; then
        sed -n '/Latency histogram/,/^$/p' "$output_file" | grep "|" | while IFS= read -r line; do
            local pct=$(echo "$line" | awk '{print $1}')
            local lat=$(echo "$line" | awk '{print $2}' | sed 's/ms//g')
            echo "$engine,$test_name,$threads,$table_size,$iteration,$pct,$lat" >> "$LATENCY_CSV"
        done
    fi

    echo ""
    echo "[OK] $engine $test_name: TPS=$tps, QPS=$qps, Latency avg=${lat_avg}ms p95=${lat_p95}ms max=${lat_max}ms | Data: ${size_after_prepare_mb}->${size_after_run_mb} MB"

    # We cleanup tables
    sysbench "$test" \
        --mysql-socket="$SOCKET" \
        --mysql-user="$DB_USER" \
        --mysql-db="$DB" \
        --tables="$TABLES" \
        cleanup > /dev/null 2>&1 || true
}

# ---------------------------------------------------------------------------
# Server management (only when DATA_DIR is set)
# ---------------------------------------------------------------------------

# We build the mariadbd argument list (used by start_server and check_and_restart_server)
build_server_args() {
    SERVER_ARGS=(
        --no-defaults
        --basedir="${BUILD_DIR}"
        --datadir="$DATA_DIR"
        --socket="$SOCKET"
        --pid-file="$PID_FILE"
        --log-error="$ERROR_LOG"
        --plugin-dir="${PLUGIN_DIR:-${BUILD_DIR}/storage/tidesdb}"
        --plugin-maturity=experimental
        --plugin-load-add=ha_tidesdb.so
        --tidesdb-flush-threads="$TIDESDB_FLUSH_THREADS"
        --tidesdb-compaction-threads="$TIDESDB_COMPACT_THREADS"
        --tidesdb-block-cache-size="$TIDESDB_BLOCK_CACHE"
        --tidesdb-max-open-sstables="$TIDESDB_MAX_SSTABLES"
        --innodb=ON
        --innodb-data-home-dir="$INNODB_DIR"
        --innodb-log-group-home-dir="$INNODB_DIR"
        --innodb-buffer-pool-size="$INNODB_BUFFER_POOL"
        --innodb-log-file-size=64M
        --innodb-flush-log-at-trx-commit="${INNODB_FLUSH:-0}"
        --skip-grant-tables
        --skip-networking
        --user="$(whoami)"
    )
}

# We start mariadbd in the background and wait for it to accept connections.
start_server() {
    "$MYSQLD" "${SERVER_ARGS[@]}" &
    echo -n "Waiting for server"
    for i in {1..30}; do
        if "$MYSQL_BIN" -S "$SOCKET" -u "$DB_USER" -e "SELECT 1" >/dev/null 2>&1; then
            echo " OK"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo " FAILED"
    echo "Check error log: $ERROR_LOG"
    tail -20 "$ERROR_LOG" 2>/dev/null
    return 1
}

# We stop a running server using its PID file.
stop_server() {
    [ -f "$PID_FILE" ] || return 0
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null) || return 0
    kill -0 "$pid" 2>/dev/null || return 0
    echo "Stopping existing server (PID: $pid)..."
    kill "$pid" 2>/dev/null
    for i in {1..10}; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null
        sleep 1
    fi
    rm -f "$PID_FILE" "$SOCKET" 2>/dev/null
    # Remove TidesDB lock file so the next start can open the database
    rm -f "$TIDESDB_DIR/LOCK" 2>/dev/null
}

# We check if the server is alive; restart if needed.
check_and_restart_server() {
    [ -z "$DATA_DIR" ] && return 0
    "$MYSQL_BIN" -S "$SOCKET" -u "$DB_USER" -e "SELECT 1" >/dev/null 2>&1 && return 0
    echo "  [WARN] Server is down, restarting..."
    rm -f "$PID_FILE" "$SOCKET" "$TIDESDB_DIR/LOCK" 2>/dev/null
    if ! start_server; then
        return 1
    fi
    "$MYSQL_BIN" -S "$SOCKET" -u "$DB_USER" -e "CREATE DATABASE IF NOT EXISTS $DB" 2>/dev/null || true
    return 0
}

SERVER_STARTED=0
if [ -n "$DATA_DIR" ]; then
    # Custom I/O directories for each engine (allows placing on different disks)
    # TidesDB plugin derives its data dir as - parent_of_datadir/tidesdb_data
    TIDESDB_DIR="$(dirname "$DATA_DIR")/tidesdb_data"
    INNODB_DIR="${INNODB_DATA_DIR:-${DATA_DIR}}"

    PID_FILE="${DATA_DIR}/mysqld.pid"
    ERROR_LOG="${DATA_DIR}/mysqld.err"
    MYSQLD="${MYSQLD:-${BUILD_DIR}/sql/mariadbd}"

    stop_server

    # Wipe everything for a clean start
    echo "Cleaning data directories for fresh benchmark..."
    rm -rf "$DATA_DIR" "$TIDESDB_DIR"
    mkdir -p "$DATA_DIR" "$TIDESDB_DIR" "$INNODB_DIR"

    # Initialize data directory
    echo "Initializing data directory: $DATA_DIR"
    "${BUILD_DIR}/scripts/mariadb-install-db" \
        --no-defaults \
        --basedir="${BUILD_DIR}" \
        --datadir="$DATA_DIR" \
        --user="$(whoami)" 2>&1 | tail -3

    echo "Starting MariaDB server with custom data directories..."
    echo "  MariaDB data:  $DATA_DIR"
    echo "  InnoDB data:   $INNODB_DIR"
    echo "  TidesDB data:  $TIDESDB_DIR"

    build_server_args
    if start_server; then
        SERVER_STARTED=1
    else
        exit 1
    fi

    echo "Creating test database..."
    "$MYSQL_BIN" -S "$SOCKET" -u "$DB_USER" -e "CREATE DATABASE IF NOT EXISTS $DB" 2>/dev/null || true
fi

# We ensure test database exists (even if server was already running)
"$MYSQL_BIN" -S "$SOCKET" -u "$DB_USER" -e "CREATE DATABASE IF NOT EXISTS $DB" 2>/dev/null || true

# We count total tests for progress
ENGINES="${ENGINES:-InnoDB TidesDB}"
num_engines=$(echo $ENGINES | wc -w)
num_sizes=$(echo $TABLE_SIZES | wc -w)
num_threads=$(echo $THREAD_COUNTS | wc -w)
num_workloads=$(echo $WORKLOADS | wc -w)
total_tests=$((num_engines * num_sizes * num_threads * num_workloads * ITERATIONS))

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Sysbench TidesDB vs InnoDB Comprehensive OLTP Benchmark"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""
echo "Configuration:"
echo "  Socket:           $SOCKET"
echo "  Data directory:   ${DATA_DIR:-default (MTR)}"
if [ -n "$DATA_DIR" ]; then
    echo "  InnoDB I/O dir:   ${INNODB_DIR:-$DATA_DIR}"
    echo "  TidesDB I/O dir:  ${TIDESDB_DIR}"
    echo "  TidesDB table:    SYNC_MODE=${TIDESDB_SYNC_MODE:-0} USE_BTREE=${TIDESDB_USE_BTREE:-0} COMPRESSION=${TIDESDB_COMPRESSION}"
    echo "  TidesDB global:   flush_threads=${TIDESDB_FLUSH_THREADS} compaction_threads=${TIDESDB_COMPACT_THREADS} block_cache=${TIDESDB_BLOCK_CACHE} max_sstables=${TIDESDB_MAX_SSTABLES}"
fi
echo "  Tables:           $TABLES"
echo "  Table sizes:      $TABLE_SIZES"
echo "  Thread counts:    $THREAD_COUNTS"
echo "  Duration:         ${TIME}s per test"
echo "  Warmup:           ${WARMUP}s per test"
echo "  Iterations:       $ITERATIONS per configuration"
echo "  Report interval:  ${REPORT_INTERVAL}s"
echo "  Engines:          $ENGINES"
echo "  Workloads:        $WORKLOADS"
echo "  Total tests:      $total_tests"
echo "  Output directory: $OUTPUT_DIR"
echo ""

current_test=0
for table_size in $TABLE_SIZES; do
    for threads in $THREAD_COUNTS; do
        for engine in $ENGINES; do
            for workload in $WORKLOADS; do
                for iteration in $(seq 1 $ITERATIONS); do
                    current_test=$((current_test + 1))
                    echo ""
                    echo "▓▓▓ Test $current_test of $total_tests ▓▓▓"

                    # We need to ensure server is alive before each test
                    if ! check_and_restart_server; then
                        echo "  [ERROR] Server could not be restarted, skipping remaining tests"
                        break 5
                    fi

                    run_sysbench_test "$engine" "$workload" "$threads" "$table_size" "$iteration" || echo "  (skipping due to error)"
                done
            done
        done
    done
done

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Benchmark Complete - Summary"
echo "═══════════════════════════════════════════════════════════════════════"
echo ""
echo "Tests completed: $current_test of $total_tests"
echo ""

echo "Top results (sorted by TPS):"
head -1 "$SUMMARY_CSV"
tail -n +2 "$SUMMARY_CSV" | sort -t',' -k5 -rn | head -20
echo ""

echo "Results saved to:"
echo "  Summary:     $SUMMARY_CSV"
echo "  Detail:      $DETAIL_CSV"
echo "  Latency:     $LATENCY_CSV"
echo "  Sizes:       $SIZES_CSV"
echo "  Raw output:  $OUTPUT_DIR/*.txt"
echo ""
echo "To analyze results:"
echo "  Compare engines by workload"
echo "  cat $SUMMARY_CSV | column -t -s','"
echo ""

exit 0
