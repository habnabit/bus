extern crate async_bus;
extern crate futures;

#[macro_use]
extern crate log;

#[macro_use]
extern crate proptest;

extern crate tokio;

use futures::{future, stream};
use futures::prelude::*;
use proptest::prelude::*;
use std::sync;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};


fn delay() -> BoxedStrategy<Duration> {
    (1..125u64)
        .prop_map(Duration::from_millis)
        .boxed()
}

#[derive(Debug, Clone, Default)]
struct ExpectedReader {
    delays: Vec<Duration>,
    reads: usize,
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
    fn spawned(self, bus_reader: async_bus::BusReader<Instant>, shared: sync::Arc<Shared>) -> impl Future<Item = (), Error = ()> {
        let reader = self.id;
        stream::iter_ok(self.delays)
            .zip(bus_reader)
            .map_err(|_| unreachable!())
            .for_each(move |(delay, sent_at)| -> Box<Future<Item = (), Error = tokio::timer::Error> + Send> {
                info!("received item to {} from {:?}", reader, sent_at.elapsed());
                if shared.fuse.load(sync::atomic::Ordering::Relaxed) {
                    return Box::new(future::err(tokio::timer::Error::shutdown()));
                }
                let received_at = Instant::now();
                shared.items.lock().unwrap().push(SentItem {
                    reader, sent_at, received_at,
                });
                Box::new(tokio::timer::Delay::new(received_at + delay))
            })
            .map_err(|e| info!("timer failure {:?}", e))
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
    prop::collection::vec(step, 1..25)
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
            let readers = if max_reads > 0 {
                let read_indices = possible_reads.iter().enumerate()
                    .filter_map(|(e, o)| if o.unwrap_or(0) > 0 { Some(e) } else { None })
                    .collect::<Vec<_>>();
                prop::collection::vec((prop::sample::select(read_indices), delay()), ..(max_reads + 2))
                    .prop_map(move |pairs| {
                        let mut readers: BTreeMap<usize, ExpectedReader> = Default::default();
                        for (idx, delay) in pairs {
                            readers.entry(idx).or_insert_with(Default::default).delays.push(delay);
                        }
                        for (idx, expected) in readers.iter_mut() {
                            expected.reads = expected.delays.len().min(possible_reads[*idx].unwrap_or(0) as usize);
                        }
                        readers
                    })
                    .boxed()
            } else {
                Just(BTreeMap::new()).boxed()
            };
            (readers, Just(steps))
        })
        .prop_map(|(mut readers, mut steps)| {
            let mut expected_counts = vec![0; steps.len()];
            for (e, step) in steps.iter_mut().enumerate() {
                use RunStep::*;
                if let SpawnReader(reader) = step {
                    if let Some(expected) = readers.remove(&e) {
                        reader.delays = expected.delays;
                        expected_counts[e] = expected.reads;
                    }
                }
            }
            StepsAndExpected { expected_counts, steps }
        })
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
                info!("\ndeadline failure\ntimeout: {:?}\ndeadline elapsed: {:?}\ntimer error: {:?}\n",
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

const MAX_TEST_TIME: Duration = Duration::from_millis(7500);

proptest! {
    #[test]
    fn it_delivers_all_messages_async(steps in run_steps(), buffer in 1..15usize) {
        let shared: sync::Arc<Shared> = Default::default();
        let tokio_shared = shared.clone();
        let StepsAndExpected { steps, expected_counts } = steps;
        let started_at = Instant::now();

        tokio::run(future::lazy(move || {
            info!("run start");
            let shared = tokio_shared.clone();
            spawn_deadlined(MAX_TEST_TIME, shared.clone(), {
                let shared = shared.clone();
                stream::iter_ok(steps)
                    .fold(async_bus::Bus::new(buffer), move |mut bus, step| -> Box<Future<Item = bus::Bus<_>, Error = _> + Send> {
                        use RunStep::*;
                        let now = Instant::now();
                        info!("current step: {:?} @{:?}", step, started_at.elapsed());
                        match step {
                            DoSend => Box::new({
                                bus.send(now)
                                    .map_err(|_| unreachable!())
                            }),
                            SpawnReader(reader) => Box::new({
                                spawn_deadlined(
                                    MAX_TEST_TIME, shared.clone(),
                                    reader.spawned(bus.add_rx(), shared.clone()));
                                Ok(bus).into_future()
                            }),
                            Wait(delay) => Box::new({
                                tokio::timer::Delay::new(now + delay)
                                    .map(move |()| bus)
                                    .map_err(|_: tokio::timer::Error| info!("delay failed"))
                            }),
                        }
                    })
                    .and_then(move |mut bus| {
                        info!("ran all the steps @{:?}", started_at.elapsed());
                        future::poll_fn(move || bus.close())
                            .map_err(|_| unreachable!())
                    })
                    .map(move |()| info!("bus closed @{:?}", started_at.elapsed()))
            });

            Ok(())
        }));

        let run_time = started_at.elapsed();
        let shared = sync::Arc::try_unwrap(shared)
            .unwrap_or_else(|e| {
                info!("couldn't unwrap arc: {:?}", e);
                panic!("arc still active");
            });
        let interrupted_run_time = if shared.fuse.into_inner() || run_time > MAX_TEST_TIME {
            info!("timed out");
            Some(run_time)
        } else {
            info!("didn't time out");
            None
        };
        let items = shared.items.into_inner().unwrap();
        let mut actual_reads = vec![0; expected_counts.len()];
        for item in items {
            actual_reads[item.reader] += 1;
        }
        info!("reads match: {:?}", actual_reads == expected_counts);
        prop_assert_eq!((actual_reads, interrupted_run_time), (expected_counts, None));
    }
}
