use clap::Parser;
use crossbeam_utils::thread;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use std::path::Path;
use std::sync::{Arc, Mutex};

const MAX_KEY_SIZE: u8 = 47;
const MAX_VALUE_SIZE: u8 = 8;

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
    #[clap(short, long, default_value = "50000")]
    pub ops_per_round: u32,

    /// Threads
    #[clap(short, long, default_value = "4")]
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

struct Stats {
    keys_added: usize,
    keys_deleted: usize,
    stop: bool,
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

    let stats = Arc::new(Mutex::new(Stats {
        keys_added: 0,
        keys_deleted: 0,
        stop: false,
    }));

    thread::scope(|s| {
        for i in 0..opts.threads {
            let thread_id = i;
            let seed = opts.seed;
            let state = &state;
            let stats = Arc::clone(&stats);
            s.spawn(move |_| {
                thread_worker(state, thread_id, seed, stats);
            });
        }
    })
    .unwrap();
}

fn thread_worker(state: &SharedState, thread_id: u8, seed: u64, stats: Arc<Mutex<Stats>>) {
    state.db.register_thread();
    let mut rng = Pcg64::seed_from_u64(seed);
    let mut key_buffer = [0u8; MAX_KEY_SIZE as usize];
    let mut value_buffer = [0u8; MAX_VALUE_SIZE as usize];

    for round in 0.. {
        let min_to_write = 3 * state.ops_per_round / 4;
        let max_to_write = state.ops_per_round;
        let num_to_write = rng.gen_range(min_to_write..max_to_write);

        do_random_inserts(
            state.db,
            num_to_write as usize,
            &mut rng,
            &mut key_buffer,
            &mut value_buffer,
        );

        do_random_lookups(
            state.db,
            (state.ops_per_round / 10) as usize,
            &mut rng,
            &mut key_buffer,
        );

        let num_deleted = if round < 100 {
            0
        } else {
            // pick a random start key for a range delete
            let mut start_key = [0u8; 4];
            rand_fill_buffer(&mut rng, &mut start_key);

            let num_to_delete = state.ops_per_round - num_to_write;
            do_range_delete(state.db, &start_key, num_to_delete as usize)
        };

        let live_keys = {
            let mut stats = stats.lock().unwrap();
            if stats.stop {
                break;
            }
            stats.keys_deleted += num_deleted;
            stats.keys_added += num_to_write as usize;
            stats.keys_added - stats.keys_deleted
        };

        if thread_id == 0 {
            let tuple_size_on_disk = (1 + MAX_KEY_SIZE + MAX_VALUE_SIZE + 8) as usize;
            let live_tuple_bytes = tuple_size_on_disk * live_keys;
            let actual_space_used = get_splinter_bytes_used(state.data_file_path);
            eprintln!(
                "round {}: live data: {:06} MiB, actual used space: {:06} MiB",
                round,
                live_tuple_bytes / MEGA,
                actual_space_used / MEGA,
            );
            if actual_space_used + 512 * MEGA >= state.disk_capacity_gib as usize * GIGA {
                eprintln!("actual space nearly filled disk space.  success.");
                let mut stats = stats.lock().unwrap();
                stats.stop = true;
                break;
            }
        };
    }
    state.db.deregister_thread();
}

fn do_random_lookups(
    db: &splinterdb_rs::KvsbDB,
    count: usize,
    rng: &mut Pcg64,
    key_buffer: &mut [u8],
) {
    for _ in 0..count {
        rand_fill_buffer(rng, key_buffer);
        match db.lookup(key_buffer).unwrap() {
            splinterdb_rs::LookupResult::Found(_) => (),
            splinterdb_rs::LookupResult::FoundTruncated(_) => panic!("truncated result"),
            splinterdb_rs::LookupResult::NotFound => (),
        }
    }
}

fn do_range_delete(db: &splinterdb_rs::KvsbDB, start_key: &[u8], count: usize) -> usize {
    let mut to_delete = vec![[0u8; MAX_KEY_SIZE as usize]; count];

    {
        // scope the iter
        let mut iter = db.range(Some(start_key)).unwrap();
        for i in 0..count {
            match iter.next() {
                Ok(Some(&splinterdb_rs::IteratorResult { key, value: _ })) => {
                    let mut dst = to_delete[i];
                    dst[..].copy_from_slice(key);
                }
                Ok(None) => {
                    eprintln!(
                        "trying to collect {} items for naive range delete but only got {}",
                        count, i
                    );
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
