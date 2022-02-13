use clap::Parser;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use std::path::Path;

const MAX_KEY_SIZE: u8 = 40;
const MAX_VALUE_SIZE: u8 = 216;

// Intended to reproduce https://github.com/vmware/splinterdb/issues/201
// but currently hitting an oversized-compaction assert
#[derive(Parser)]
#[clap(name = "assert-repro", version = "0.1")]
struct Opts {
    #[clap(short, long)]
    file: String,

    /// Size of in-memory cache, in GiB
    #[clap(short, long, default_value = "3")]
    pub cache_gib: u8,

    /// Size of file to use on disk, in GiB
    #[clap(short, long, default_value = "128")]
    pub disk_gib: u8,

    /// Operations per round
    #[clap(short, long, default_value = "200000")]
    pub ops_per_round: u32,

    /// Threads
    #[clap(short, long, default_value = "5")]
    pub threads: u8,

    /// Random seed
    #[clap(short, long, default_value = "42")]
    pub seed: u64,
}

const MEGA: usize = 1024 * 1024;
const GIGA: usize = 1024 * MEGA;

struct SharedState<'a> {
    db: &'a splinterdb_rs::KvsbDB,
    ops_per_round: u32,
    disk_capacity_gib: u8,
    data_file_path: &'a str,
}

fn multi_threaded_repro(opts: Opts) {
    println!("{}", splinterdb_rs::get_version());
    if let Err(e) = std::fs::remove_file(&opts.file) {
        if e.kind() != std::io::ErrorKind::NotFound {
            panic!("{}", e);
        }
    }

    let db_config = splinterdb_rs::DBConfig {
        cache_size_bytes: GIGA * opts.cache_gib as usize,
        disk_size_bytes: GIGA * opts.disk_gib as usize,
        max_key_size: MAX_KEY_SIZE,
        max_value_size: MAX_VALUE_SIZE,
    };
    let db = splinterdb_rs::db_create(&opts.file, &db_config).unwrap();

    let state = SharedState {
        db: &db,
        ops_per_round: opts.ops_per_round,
        disk_capacity_gib: opts.disk_gib,
        data_file_path: &opts.file,
    };
    thread_worker_writer(&state, opts.seed);
}

fn thread_worker_writer(state: &SharedState, seed: u64) {
    let mut rng = Pcg64::seed_from_u64(seed);
    let mut key_buffer = [0u8; MAX_KEY_SIZE as usize];
    let mut value_buffer = [0u8; MAX_VALUE_SIZE as usize];

    let mut live_keys = 0;

    for round in 0u32.. {
        let min_ops = state.ops_per_round / 4;
        let max_ops = state.ops_per_round;
        let num_ops = rng.gen_range(min_ops..max_ops) as usize;

        if round < 9 {
            do_random_inserts(
                state.db,
                num_ops,
                &mut rng,
                &mut key_buffer,
                &mut value_buffer,
            );
            live_keys += num_ops;
        } else {
            // empty start key
            let mut start_key = [0u8; 6];
            rand_fill_buffer(&mut rng, &mut start_key);
            let num_deleted = do_range_delete(state.db, &start_key, 2 * num_ops);
            if num_deleted >= live_keys {
                break;
            }
            live_keys -= num_deleted;
        }

        let tuple_size_on_disk = (1 + MAX_KEY_SIZE as u16 + MAX_VALUE_SIZE as u16 + 8) as usize;
        let live_tuple_bytes = tuple_size_on_disk * live_keys;
        let actual_space_used = get_splinter_bytes_used(state.data_file_path);
        eprintln!(
            "round {}: live data: {:06} MiB, space used: {:06} MiB",
            round,
            live_tuple_bytes / MEGA,
            actual_space_used / MEGA,
        );
        if actual_space_used + 512 * MEGA >= state.disk_capacity_gib as usize * GIGA {
            eprintln!("actual space nearly filled disk space.  success.");
            break;
        }
    }
}

fn do_range_delete(db: &splinterdb_rs::KvsbDB, start_key: &[u8], count: usize) -> usize {
    let mut to_delete = vec![[0u8; MAX_KEY_SIZE as usize]; count];

    // collect items to delete
    {
        // RAII iterator, so scope it so that it is cleaned up before we begin deletes
        let mut iter = db.range(Some(start_key)).unwrap();
        for i in 0..count {
            match iter.next() {
                Ok(Some(&splinterdb_rs::IteratorResult { key, value: _ })) => {
                    to_delete[i][..].copy_from_slice(key);
                }
                Ok(None) => {
                    to_delete.truncate(i);
                    break;
                }
                Err(e) => {
                    panic!("naive range delete item collection errored: {}", e);
                }
            }
        }
    }

    for key in &to_delete {
        match db.lookup(&key[..]).unwrap() {
            splinterdb_rs::LookupResult::Found(_) => (),
            splinterdb_rs::LookupResult::FoundTruncated(_) => panic!("truncated result"),
            splinterdb_rs::LookupResult::NotFound => panic!("not found key expected to delete"),
        }
        db.delete(&key[..]).unwrap();
    }
    to_delete.len()
}

fn do_random_inserts(
    db: &splinterdb_rs::KvsbDB,
    count: usize,
    rng: &mut Pcg64,
    key_buffer: &mut [u8],
    value_buffer: &mut [u8],
) {
    for _ in 0..count {
        rand_fill_buffer(rng, key_buffer);
        rand_fill_buffer(rng, value_buffer);
        db.insert(key_buffer, value_buffer).unwrap();
    }
}

fn rand_fill_buffer(rng: &mut Pcg64, to_fill: &mut [u8]) {
    for x in to_fill.iter_mut() {
        *x = rng.gen();
    }
    // to match the foundationdb load generator
    to_fill[0] = b'3';
    to_fill[1] = b'f';
}

fn get_splinter_bytes_used<P: AsRef<Path>>(path: P) -> usize {
    // splinter creates sparse files, so this is how
    // we determine actual disk usage
    use std::fs;
    use std::os::linux::fs::MetadataExt;

    let meta = fs::metadata(path).unwrap();
    // see https://man7.org/linux/man-pages/man2/stat.2.html
    const ST_BLOCKS_UNIT_SIZE: usize = 512;
    meta.st_blocks() as usize * ST_BLOCKS_UNIT_SIZE
}

fn main() {
    let opts: Opts = Opts::parse();
    multi_threaded_repro(opts);
}
