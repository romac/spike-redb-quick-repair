//! Benchmark comparing `quick_repair(true)` vs `quick_repair(false)` impact on write performance using a `redb` database.

use rand::Rng;
use redb::{Database, Error, TableDefinition};
use std::fs;
use std::time::{Duration, Instant};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("benchmark_data");

// Configuration
const VALUE_SIZE: usize = 4096; // 4KB per value
const BATCH_SIZE: usize = 1000; // Number of inserts per transaction
const BENCHMARK_WRITES: usize = 10000; // Number of writes for benchmarking

struct BenchmarkStats {
    total_duration: Duration,
    avg_write_time: Duration,
    min_write_time: Duration,
    max_write_time: Duration,
    writes_per_second: f64,
}

impl BenchmarkStats {
    fn new(durations: &[Duration]) -> Self {
        let total_duration: Duration = durations.iter().sum();
        let count = durations.len() as f64;
        let avg_write_time = total_duration / durations.len() as u32;
        let min_write_time = *durations.iter().min().unwrap();
        let max_write_time = *durations.iter().max().unwrap();
        let writes_per_second = count / total_duration.as_secs_f64();

        Self {
            total_duration,
            avg_write_time,
            min_write_time,
            max_write_time,
            writes_per_second,
        }
    }

    fn print(&self, label: &str) {
        println!("\n{}", "=".repeat(60));
        println!("{}", label);
        println!("{}", "=".repeat(60));
        println!("Total duration:      {:?}", self.total_duration);
        println!("Average write time:  {:?}", self.avg_write_time);
        println!("Min write time:      {:?}", self.min_write_time);
        println!("Max write time:      {:?}", self.max_write_time);
        println!("Writes per second:   {:.2}", self.writes_per_second);
        println!("{}", "=".repeat(60));
    }
}

fn generate_random_value(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random::<u8>()).collect()
}

fn get_file_size(path: &str) -> Result<u64, std::io::Error> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.len())
}

fn fill_database(db_path: &str, target_size_gb: u64) -> Result<u64, Error> {
    println!("\n{}", "=".repeat(60));
    println!("Filling database: {}", db_path);
    println!("{}", "=".repeat(60));

    let db = Database::builder()
        .set_cache_size(1024 * 1024 * 1024) // 1GB cache
        .set_repair_callback(move |session| {
            println!("Repair progress: {:.2}%", session.progress() * 100.0);
        })
        .create(db_path)?;

    let target_bytes = target_size_gb * 1024 * 1024 * 1024;
    let mut key_counter = 0u64;
    let mut total_bytes = 0u64;
    let mut batch_counter = 0;

    let start_time = Instant::now();

    while total_bytes < target_bytes {
        let write_txn = db.begin_write()?;

        {
            let mut table = write_txn.open_table(TABLE)?;

            for _ in 0..BATCH_SIZE {
                let value = generate_random_value(VALUE_SIZE);
                table.insert(key_counter, value.as_slice())?;
                key_counter += 1;
                total_bytes += VALUE_SIZE as u64;
            }
        }

        write_txn.commit()?;

        batch_counter += 1;

        if batch_counter % 100 == 0 {
            let current_size = get_file_size(db_path).unwrap_or(0);
            let current_written = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            let current_gb = current_size as f64 / (1024.0 * 1024.0 * 1024.0);
            let elapsed = start_time.elapsed();
            println!(
                "Progress: {:.2} GB written, DB size {:.2} GB, {} records, elapsed: {:?}",
                current_written, current_gb, key_counter, elapsed
            );
        }
    }

    let final_size = get_file_size(db_path).unwrap_or(0);
    let final_gb = final_size as f64 / (1024.0 * 1024.0 * 1024.0);
    let elapsed = start_time.elapsed();

    println!("\nDatabase filled successfully!");
    println!("Final size: {:.2} GB", final_gb);
    println!("Total records: {}", key_counter);
    println!("Time taken: {:?}", elapsed);

    Ok(key_counter)
}

fn benchmark_writes(
    db_path: &str,
    start_key: u64,
    num_writes: usize,
    quick_repair: bool,
) -> Result<BenchmarkStats, Error> {
    println!("\n{}", "=".repeat(60));
    println!(
        "Benchmarking writes on: {} (quick_repair={})",
        db_path, quick_repair
    );
    println!("Number of writes: {}", num_writes);
    println!("{}", "=".repeat(60));

    let db = Database::builder()
        .set_cache_size(1024 * 1024 * 1024) // 1GB cache
        .create(db_path)?;

    let mut durations = Vec::with_capacity(num_writes);
    let mut key_counter = start_key;

    for i in 0..num_writes {
        let value = generate_random_value(VALUE_SIZE);

        let start = Instant::now();

        let mut write_txn = db.begin_write()?;
        write_txn.set_quick_repair(quick_repair);
        {
            let mut table = write_txn.open_table(TABLE)?;
            table.insert(key_counter, value.as_slice())?;
        }
        write_txn.commit()?;

        let duration = start.elapsed();
        durations.push(duration);

        key_counter += 1;

        if (i + 1) % 1000 == 0 {
            println!("Completed {} / {} writes", i + 1, num_writes);
        }
    }

    Ok(BenchmarkStats::new(&durations))
}

fn benchmark_batch_writes(
    db_path: &str,
    start_key: u64,
    num_batches: usize,
    batch_size: usize,
    quick_repair: bool,
) -> Result<BenchmarkStats, Error> {
    println!("\n{}", "=".repeat(60));
    println!(
        "Benchmarking batch writes on: {} (quick_repair={})",
        db_path, quick_repair
    );
    println!(
        "Number of batches: {}, batch size: {}",
        num_batches, batch_size
    );
    println!("{}", "=".repeat(60));

    let db = Database::builder()
        .set_cache_size(1024 * 1024 * 1024) // 1GB cache
        .create(db_path)?;

    let mut durations = Vec::with_capacity(num_batches);
    let mut key_counter = start_key;

    for i in 0..num_batches {
        let start = Instant::now();

        let mut write_txn = db.begin_write()?;
        write_txn.set_quick_repair(quick_repair);
        {
            let mut table = write_txn.open_table(TABLE)?;
            for _ in 0..batch_size {
                let value = generate_random_value(VALUE_SIZE);
                table.insert(key_counter, value.as_slice())?;
                key_counter += 1;
            }
        }
        write_txn.commit()?;

        let duration = start.elapsed();
        durations.push(duration);

        if (i + 1) % 100 == 0 {
            println!("Completed {} / {} batches", i + 1, num_batches);
        }
    }

    Ok(BenchmarkStats::new(&durations))
}

fn cleanup_db(db_path: &str) {
    if let Err(e) = fs::remove_file(db_path) {
        eprintln!("Warning: Could not remove {}: {}", db_path, e);
    }
}

/// Spike to benchmark redb write performance with different quick_repair settings
#[derive(argh::FromArgs)]
struct Args {
    /// target database size in GiB (default: 10)
    #[argh(option, default = "10")]
    target_size_gb: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Args = argh::from_env();
    let target_size_gb = args.target_size_gb;

    println!("\n{}", "█".repeat(60));
    println!("REDB WRITE PERFORMANCE BENCHMARK");
    println!("Comparing set_quick_repair(true) vs set_quick_repair(false)");
    println!("{}", "█".repeat(60));

    // Database paths
    let db_quick_repair_false = "benchmark_quick_repair_false.redb";
    let db_quick_repair_true = "benchmark_quick_repair_true.redb";

    // Clean up any existing databases
    println!("\nCleaning up existing database files...");
    cleanup_db(db_quick_repair_false);
    cleanup_db(db_quick_repair_true);

    println!("\n{}", "█".repeat(60));
    println!("PHASE 1: Filling databases with {target_size_gb} GiB of data");
    println!("{}", "█".repeat(60));

    let max_key_false = fill_database(db_quick_repair_false, target_size_gb)?;
    let max_key_true = fill_database(db_quick_repair_true, target_size_gb)?;

    println!("\n{}", "█".repeat(60));
    println!("PHASE 2: Benchmarking individual write performance");
    println!("{}", "█".repeat(60));

    // Benchmark individual writes on quick_repair = false
    let stats_individual_false = benchmark_writes(
        db_quick_repair_false,
        max_key_false,
        BENCHMARK_WRITES,
        false,
    )?;

    // Benchmark individual writes on quick_repair = true
    let stats_individual_true =
        benchmark_writes(db_quick_repair_true, max_key_true, BENCHMARK_WRITES, true)?;

    // println!("\n{}", "█".repeat(60));
    // println!("PHASE 3: Benchmarking batch write performance");
    // println!("{}", "█".repeat(60));
    //
    // // Benchmark batch writes on quick_repair = true
    // let stats_batch_true = benchmark_batch_writes(
    //     db_quick_repair_true,
    //     max_key_true + BENCHMARK_WRITES as u64,
    //     1000,
    //     100,
    //     true,
    // )?;
    //
    // // Benchmark batch writes on quick_repair = false
    // let stats_batch_false = benchmark_batch_writes(
    //     db_quick_repair_false,
    //     max_key_false + BENCHMARK_WRITES as u64,
    //     1000,
    //     100,
    //     false,
    // )?;

    // Print all results
    println!("\n\n");
    println!("{}", "█".repeat(60));
    println!("BENCHMARK RESULTS SUMMARY");
    println!("{}", "█".repeat(60));

    stats_individual_false.print("Individual Writes - quick_repair(false)");
    stats_individual_true.print("Individual Writes - quick_repair(true)");

    println!("\n{}", "-".repeat(60));
    println!("Individual Write Performance Comparison:");
    let speedup_individual =
        stats_individual_false.writes_per_second / stats_individual_true.writes_per_second;
    println!(
        "quick_repair(false) is {:.2}x faster than quick_repair(true)",
        speedup_individual
    );
    let latency_diff = stats_individual_true.avg_write_time.as_micros() as i64
        - stats_individual_false.avg_write_time.as_micros() as i64;
    println!("Latency difference: {} μs per write", latency_diff);
    println!("{}", "-".repeat(60));

    // stats_batch_true.print("Batch Writes (100 per txn) - quick_repair(true)");
    // stats_batch_false.print("Batch Writes (100 per txn) - quick_repair(false)");
    //
    // println!("\n{}", "-".repeat(60));
    // println!("Batch Write Performance Comparison:");
    // let speedup_batch = stats_batch_false.writes_per_second / stats_batch_true.writes_per_second;
    // println!(
    //     "quick_repair(false) is {:.2}x faster than quick_repair(true)",
    //     speedup_batch
    // );
    // let latency_diff_batch = stats_batch_true.avg_write_time.as_micros() as i64
    //     - stats_batch_false.avg_write_time.as_micros() as i64;
    // println!(
    //     "Latency difference: {} μs per batch commit",
    //     latency_diff_batch
    // );
    // println!("{}", "-".repeat(60));

    println!("\n{}", "█".repeat(60));
    println!("BENCHMARK COMPLETE");
    println!("{}", "█".repeat(60));

    println!("\nDatabase files preserved for inspection:");
    println!("  - {}", db_quick_repair_true);
    println!("  - {}", db_quick_repair_false);

    Ok(())
}
