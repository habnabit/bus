extern crate bus;
extern crate futures;
#[macro_use]
extern crate proptest;
extern crate tokio;

use futures::{future, stream};
use futures::prelude::*;
use proptest::prelude::*;
use std::{iter, mem, sync, thread};
use std::time::{Duration, Instant};


fn delay() -> BoxedStrategy<Duration> {
    (1..1250u64)
        .prop_map(Duration::from_millis)
        .boxed()
}

#[derive(Debug, Clone)]
struct TestReader {
    id: usize,
    delays: Vec<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
struct SentItem {
    reader: usize,
    sent_at: Instant,
    received_at: Instant,
}

impl TestReader {
    fn spawned(self, bus_reader: bus::BusReader<Instant, futures::task::Task>, shared: sync::Arc<Shared>) -> impl Future<Item = (), Error = ()> {
        let reader = self.id;
        stream::iter_ok(self.delays)
            .zip(bus_reader)
            .map_err(|_| unreachable!())
            .for_each(move |(delay, sent_at)| -> Box<Future<Item = (), Error = tokio::timer::Error> + Send> {
                eprintln!("received item to {} from {:?}", reader, sent_at.elapsed());
                if shared.fuse.load(sync::atomic::Ordering::Relaxed) {
                    return Box::new(future::err(tokio::timer::Error::shutdown()));
                }
                let received_at = Instant::now();
                shared.items.lock().unwrap().push(SentItem {
                    reader, sent_at, received_at,
                });
                Box::new(tokio::timer::Delay::new(received_at + delay))
            })
            .map_err(|e| eprintln!("timer failure"))
    }

    fn spawned_sync(self, bus_reader: bus::BusReader<Instant>, shared: sync::Arc<Shared>) -> thread::JoinHandle<()> {
        let TestReader { id: reader, delays } = self;
        thread::spawn(move || {
            for (delay, sent_at) in delays.into_iter().zip(bus_reader) {
                eprintln!("received item to {} from {:?}", reader, sent_at.elapsed());
                if shared.fuse.load(sync::atomic::Ordering::Relaxed) {
                    return;
                }
                let received_at = Instant::now();
                shared.items.lock().unwrap().push(SentItem {
                    reader, sent_at, received_at,
                });
                thread::sleep(delay);
            }
        })
    }
}

#[derive(Debug, Clone)]
enum RunStep {
    DoSend,
    SpawnReader(TestReader),
    Wait(Duration),
}

#[derive(Debug, Clone)]
struct StepsAndExpected {
    steps: Vec<RunStep>,
    expected_counts: Vec<usize>,
}

fn run_steps() -> BoxedStrategy<StepsAndExpected> {
    let step = prop_oneof![
        Just(RunStep::DoSend),
        Just(RunStep::SpawnReader(TestReader { id: 0, delays: vec![] })),
        delay().prop_map(RunStep::Wait),
    ];
    prop::collection::vec(step, 1..75)
        .prop_flat_map(|mut steps| {
            let n_steps = steps.len();
            let mut possible_reads = vec![None; n_steps];
            for (e, step) in steps.iter_mut().enumerate() {
                use RunStep::*;
                match step {
                    SpawnReader(reader) => {
                        reader.id = e;
                        possible_reads[e] = Some(0u32);
                    },
                    DoSend => {
                        possible_reads.iter_mut()
                            .filter_map(Option::as_mut)
                            .for_each(|c: &mut u32| *c += 1);
                    },
                    Wait(_) => (),
                }
            }
            let max_reads = possible_reads.iter().filter_map(|o| o.clone()).sum::<u32>() as usize;
            let reader_delays = if max_reads > 0 {
                let read_indices = possible_reads.iter().enumerate()
                    .flat_map(|(e, o)| iter::repeat(e).take(o.unwrap_or(0) as usize))
                    .collect::<Vec<_>>();
                (Just(read_indices).prop_shuffle(), prop::collection::vec(delay(), ..max_reads))
                    .prop_map(move |(mut indices, mut delays)| {
                        let mut reader_delays = vec![vec![]; n_steps];
                        while let (Some(idx), Some(delay)) = (indices.pop(), delays.pop()) {
                            reader_delays[idx].push(delay);
                        }
                        reader_delays
                    })
                    .boxed()
            } else {
                Just(vec![vec![]; n_steps]).boxed()
            };
            (reader_delays, Just(steps))
        })
        .prop_map(|(mut reader_delays, mut steps)| {
            let mut expected_counts = vec![0; steps.len()];
            for (e, step) in steps.iter_mut().enumerate() {
                use RunStep::*;
                match step {
                    SpawnReader(reader) => {
                        reader.delays = mem::replace(&mut reader_delays[e], vec![]);
                        expected_counts[e] = reader.delays.len();
                    },
                    _ => (),
                }
            }
            StepsAndExpected { expected_counts, steps }
        })
        .boxed()
}

#[derive(Debug)]
struct WholeRun {
    readers: Vec<TestReader>,
    sender_delays: Vec<Duration>,
}

fn whole_run() -> BoxedStrategy<WholeRun> {
    ((0..10usize), (0..25usize))
        .prop_flat_map(|(readers, sends)| {
            let delays = prop::collection::vec(delay(), sends);
            let reader = delays.clone().prop_map(|delays| TestReader { id: 0, delays });
            let readers = prop::collection::vec(reader, readers);
            (readers, delays)
        })
        .prop_map(|(readers, sender_delays)| WholeRun { readers, sender_delays })
        .boxed()
}

fn spawn_deadlined<F>(timeout: Duration, shared: sync::Arc<Shared>, future: F)
    where F: futures::Future<Error = ()> + Send + 'static,
{
    use futures::Future;
    let deadline = Instant::now() + timeout;
    tokio::spawn({
        tokio::timer::Deadline::new(future, deadline)
            .map(|_| ())
            .map_err(move |e| {
                eprintln!("\ndeadline failure\ntimeout: {:?}\ndeadline elapsed: {:?}\ntimer error: {:?}\n",
                    timeout, e.is_elapsed(), e.into_timer());
                shared.fuse.store(true, sync::atomic::Ordering::Relaxed);
            })
    });
}

#[derive(Debug, Default)]
struct Shared {
    fuse: sync::atomic::AtomicBool,
    items: sync::Mutex<Vec<SentItem>>,
}

proptest! {
    #[test]
    fn it_delivers_all_messages_async(steps in run_steps(), buffer in 1..15usize) {
        let shared: sync::Arc<Shared> = Default::default();
        let tokio_shared = shared.clone();
        let StepsAndExpected { steps, expected_counts } = steps;
        let started_at = Instant::now();

        tokio::run(future::lazy(move || {
            eprintln!("run start");
            let shared = tokio_shared.clone();
            spawn_deadlined(Duration::from_secs(10), shared.clone(), {
                let shared = shared.clone();
                stream::iter_ok(steps)
                    .fold(bus::Bus::new_async(buffer), move |mut bus, step| -> Box<Future<Item = bus::Bus<_, _>, Error = _> + Send> {
                        use RunStep::*;
                        let now = Instant::now();
                        eprintln!("current step: {:?} @{:?}", step, started_at.elapsed());
                        match step {
                            DoSend => Box::new({
                                bus.send(now)
                                    .map_err(|_| unreachable!())
                            }),
                            SpawnReader(reader) => Box::new({
                                spawn_deadlined(
                                    Duration::from_secs(10), shared.clone(),
                                    reader.spawned(bus.add_rx(), shared.clone()));
                                Ok(bus).into_future()
                            }),
                            Wait(delay) => Box::new({
                                tokio::timer::Delay::new(now + delay)
                                    .map(move |()| bus)
                                    .map_err(|_: tokio::timer::Error| eprintln!("delay failed"))
                            }),
                        }
                    })
                    .map(move |_| eprintln!("ran all the steps @{:?}", started_at.elapsed()))
            });

            Ok(())
        }));

        let shared = sync::Arc::try_unwrap(shared)
            .unwrap_or_else(|e| {
                eprintln!("couldn't unwrap arc: {:?}", e);
                panic!("arc still active");
            });
        let interrupted_run_time = if shared.fuse.into_inner() {
            eprintln!("timed out");
            Some(started_at.elapsed())
        } else {
            eprintln!("didn't time out");
            None
        };
        let items = shared.items.into_inner().unwrap();
        let mut actual_reads = vec![0; expected_counts.len()];
        for item in items {
            actual_reads[item.reader] += 1;
        }
        eprintln!("reads match: {:?}", actual_reads == expected_counts);
        prop_assert_eq!((actual_reads, interrupted_run_time), (expected_counts, None));
    }

    #[test]
    fn it_delivers_all_messages_sync(steps in run_steps(), buffer in 1..15usize) {
        let shared: sync::Arc<Shared> = Default::default();
        let (threads_tx, threads_rx) = sync::mpsc::channel();
        let StepsAndExpected { steps, expected_counts } = steps;
        let started_at = Instant::now();
        let mut bus = bus::Bus::new(buffer);

        let inner_shared = shared.clone();
        let inner_threads_tx = threads_tx.clone();
        let main_thread = thread::spawn(move || {
            eprintln!("run start");
            for step in steps {
                use RunStep::*;
                let now = Instant::now();
                eprintln!("current step: {:?} @{:?}", step, started_at.elapsed());
                match step {
                    DoSend => bus.broadcast(now),
                    SpawnReader(reader) => {
                        let reader = reader.spawned_sync(bus.add_rx(), inner_shared.clone());
                        inner_threads_tx.send(reader).unwrap();
                    },
                    Wait(delay) => thread::sleep(delay),
                }
                eprintln!("ran all the steps @{:?}", started_at.elapsed());
            }
        });

        threads_tx.send(main_thread).unwrap();
        drop(threads_tx);
        for thread in threads_rx.iter() {
            thread.join().unwrap();
        }

        let shared = sync::Arc::try_unwrap(shared)
            .unwrap_or_else(|e| {
                eprintln!("couldn't unwrap arc: {:?}", e);
                panic!("arc still active");
            });
        let interrupted_run_time = if shared.fuse.into_inner() {
            eprintln!("timed out");
            Some(started_at.elapsed())
        } else {
            eprintln!("didn't time out");
            None
        };
        let items = shared.items.into_inner().unwrap();
        let mut actual_reads = vec![0; expected_counts.len()];
        for item in items {
            actual_reads[item.reader] += 1;
        }
        eprintln!("reads match: {:?}", actual_reads == expected_counts);
        prop_assert_eq!((actual_reads, interrupted_run_time), (expected_counts, None));
    }
}
