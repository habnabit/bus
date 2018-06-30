extern crate bus;
extern crate futures;
#[macro_use]
extern crate proptest;
extern crate tokio;

use futures::{future, stream};
use futures::prelude::*;
use proptest::prelude::*;
use std::sync;
use std::time::{Duration, Instant};


fn delay() -> BoxedStrategy<Duration> {
    (10..200u64)
        .prop_map(Duration::from_millis)
        .boxed()
}

#[derive(Debug)]
struct TestReader {
    delays: Vec<Duration>,
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
            let reader = delays.clone().prop_map(|delays| TestReader { delays });
            let readers = prop::collection::vec(reader, readers);
            (readers, delays)
        })
        .prop_map(|(readers, sender_delays)| WholeRun { readers, sender_delays })
        .boxed()
}

fn spawn_deadlined<F>(timeout: Duration, future: F)
    where F: futures::Future + Send + 'static,
{
    use futures::Future;
    let deadline = Instant::now() + timeout;
    tokio::spawn({
        tokio::timer::Deadline::new(future, deadline)
            .map(|_| ())
            .map_err(move |e| {
                panic!("\ndeadline failure\ntimeout: {:?}\ndeadline elapsed: {:?}\ntimer error: {:?}\n",
                    timeout, e.is_elapsed(), e.into_timer());
            })
    });
}

proptest! {
    #[test]
    fn it_wakes_async_writers(run in whole_run(), buffer in 1..15usize) {
        let expected_items = run.readers
            .first().as_ref().map(|r| r.delays.len())
            .unwrap_or(0) * run.readers.len();
        let max_delay_sum = run.readers.iter()
            .map(|r| r.delays.iter())
            .chain(std::iter::once(run.sender_delays.iter()))
            .map(|i| i.sum::<Duration>())
            .max().unwrap_or_else(|| Duration::from_millis(10));
        let items = sync::Arc::new(sync::Mutex::new(vec![]));
        let tokio_items = items.clone();

        tokio::run(future::lazy(move || {
            let items = tokio_items;
            let started_at = Instant::now();
            let timeout = max_delay_sum * 2;
            let mut bus = bus::Bus::new_async(buffer);

            for (nr, reader) in run.readers.into_iter().enumerate() {
                spawn_deadlined(timeout, {
                    let items = items.clone();
                    stream::iter_ok(reader.delays)
                        .zip(bus.add_rx())
                        .for_each(move |(delay, e)| {
                            items.lock().unwrap().push((nr, e, started_at.elapsed()));
                            tokio::timer::Delay::new(Instant::now() + delay)
                                .map_err(|_: tokio::timer::Error| panic!("delay failed"))
                        })
                        .map_err(|_| unreachable!())
                });
            }

            spawn_deadlined(timeout, {
                stream::iter_ok::<_, ()>(run.sender_delays.into_iter().enumerate())
                    .and_then(|(e, delay)| {
                        tokio::timer::Delay::new(Instant::now() + delay)
                            .map_err(|_: tokio::timer::Error| panic!("delay failed"))
                            .map(move |()| e)
                    })
                    .forward(bus.sink_map_err(|_| unreachable!()))
            });

            Ok(())
        }));

        let mut items = sync::Arc::try_unwrap(items).unwrap()
            .into_inner().unwrap();
        prop_assert_eq!(items.len(), expected_items);
    }
}
