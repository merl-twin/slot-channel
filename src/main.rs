use crossbeam::channel::{bounded, unbounded, Receiver, SendError, Sender};

struct EmptySlot<T> {
    slot_sender: Sender<Slot<T>>,
    empty_slot_sender: Sender<EmptySlot<T>>,
}

struct Slot<T> {
    slot_sender: Sender<Slot<T>>,
    empty_slot_sender: Sender<EmptySlot<T>>,
    task: T,
}

#[derive(Debug)]
struct Disconnected;

impl<T> Slot<T> {
    fn new(empty_slot_storage: &Sender<EmptySlot<T>>) -> Result<Receiver<Slot<T>>, Disconnected> {
        let (tx, rx) = bounded(1);
        let slot = EmptySlot {
            slot_sender: tx,
            empty_slot_sender: empty_slot_storage.clone(),
        };
        empty_slot_storage.send(slot).map_err(|_| Disconnected)?;
        Ok(rx)
    }

    fn take(self) -> T {
        let sender = self.empty_slot_sender.clone();
        let slot = EmptySlot {
            slot_sender: self.slot_sender,
            empty_slot_sender: self.empty_slot_sender,
        };
        sender.send(slot).ok();
        self.task
    }
}

impl<T> EmptySlot<T> {
    fn send(self, task: T) -> Result<(), SendError<T>> {
        let sender = self.slot_sender.clone();
        let slot = Slot {
            slot_sender: self.slot_sender,
            empty_slot_sender: self.empty_slot_sender,
            task,
        };
        sender.send(slot).map_err(|e| SendError(e.0.take()))
    }
}

fn main() {
    let mut threads = Vec::new();
    let tm = std::time::Instant::now();
    let (empty_slot_storage, empty_slots) = unbounded();
    for i in 0..4 {
        let task_receiver = Slot::new(&empty_slot_storage).unwrap(); // because of example
        threads.push(std::thread::spawn(move || {
            //let name = format!("worker-{i}");
            //println!("[{}] ({:?}) start", name, tm.elapsed());
            for task in task_receiver {
                let task = task.take();
                //println!("[{}] ({:?}) got task {}", name, tm.elapsed(), task);
                std::mem::drop(task);
            }
            //println!("[{}] ({:?}) shutdown", name, tm.elapsed());
        }));
    }

    let (task_sender, task_receiver) = unbounded();
    threads.push(std::thread::spawn(move || {
        let name = "dispatcher";
        //println!("[{}] ({:?}) start", name, tm.elapsed());
        for task in task_receiver {
            match empty_slots.recv() {
                Ok(empty_slot) => {
                    //println!("[{}] ({:?}) dispatch task {}", name, tm.elapsed(), task);
                    if let Err(_) = empty_slot.send(task) {
                        //println!("[{}] ({:?}) dead worker detected", name, tm.elapsed());
                        // its better to resend task (can be taken from the error)
                    }
                }
                Err(_) => break, // no more slots and workers
            }
        }
        //println!("[{}] ({:?}) shutdown", name, tm.elapsed());
    }));

    let mut tasks = Vec::with_capacity(10_000_000);
    for t in 0..10_000_000 {
        tasks.push(format!("task-{t}"));
        //task_sender.send(format!("task-{t}")).ok();
    }

    let tm = std::time::Instant::now();
    println!("[main] tasking slot-channel: {:?}", tm.elapsed());

    for t in tasks {
        task_sender.send(t).ok();
    }

    // drop
    std::mem::drop(task_sender);
    std::mem::drop(empty_slot_storage); // its better to do this

    for h in threads {
        h.join().unwrap(); // propagating panic
    }
    let s = tm.elapsed();
    println!(
        "[main] DONE slot-channel: {:?} ({:.3} per sec)",
        s,
        10_000_000.0 / s.as_secs_f64()
    );

    // *******************  simple channel  **************************

    let mut tasks = Vec::with_capacity(10_000_000);
    for t in 0..10_000_000 {
        tasks.push(format!("task-{t}"));
        //task_sender.send(format!("task-{t}")).ok();
    }

    let mut threads = Vec::new();
    let tm = std::time::Instant::now();
    let (task_sender, task_receiver) = unbounded();
    for i in 0..4 {
        let task_receiver = task_receiver.clone();
        threads.push(std::thread::spawn(move || {
            for task in task_receiver {
                std::mem::drop(task);
            }
        }));
    }

    let tm = std::time::Instant::now();
    println!("[main] tasking one channel: {:?}", tm.elapsed());

    for t in tasks {
        task_sender.send(t).ok();
    }

    // drop
    std::mem::drop(task_sender);

    for h in threads {
        h.join().unwrap(); // propagating panic
    }

    let s = tm.elapsed();
    println!(
        "[main] DONE one-channel: {:?} ({:.3} per sec)",
        s,
        10_000_000.0 / s.as_secs_f64()
    );
}
