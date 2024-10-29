//! The filesystem watcher: [`WatchDispatcher`] owns a `notify` recommended
//! watcher and a background task that periodically drains settled events from
//! the [`Debouncer`] onto a channel. The debounce/coalesce state machine and
//! the event vocabulary live in [`debounce`].

pub mod debounce;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use debounce::DebouncedEventKind;
use debounce::Debouncer;
pub use notify;
use notify::{Event, RecommendedWatcher, Watcher};

pub struct WatchDispatcher {
    stop: Arc<AtomicBool>,
    watcher: RecommendedWatcher,
    _task: tokio::task::JoinHandle<()>,
}

impl WatchDispatcher {
    pub async fn new() -> Result<
        (
            WatchDispatcher,
            tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
        ),
        notify::Error,
    > {
        let debouncer = Arc::new(Mutex::new(Debouncer::default()));

        let stop = Arc::new(AtomicBool::new(false));
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();

        let task = {
            let debouncer = debouncer.clone();
            let stop = stop.clone();

            tokio::spawn(async move {
                loop {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }

                    tokio::time::sleep(Duration::from_millis(250)).await;

                    let mut debouncer = debouncer.lock().unwrap();
                    for event in debouncer.extract_finalized() {
                        let _ = event_sender.send(event);
                    }
                }
            })
        };

        let watcher = RecommendedWatcher::new(
            move |result: Result<Event, notify::Error>| {
                if let Ok(event) = result {
                    debouncer.lock().unwrap().push_raw(event);
                }
            },
            notify::Config::default(),
        )?;

        Ok((
            WatchDispatcher {
                stop,
                watcher,
                _task: task,
            },
            event_receiver,
        ))
    }

    pub fn watcher(&mut self) -> &mut RecommendedWatcher {
        &mut self.watcher
    }
}

impl Drop for WatchDispatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
