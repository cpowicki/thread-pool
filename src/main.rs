use std::{
    collections::{HashMap, VecDeque},
    thread::{sleep, JoinHandle},
    time::Duration,
};

use crossbeam::channel::*;

pub struct Task {
    op: Box<dyn Fn() + Send>,
}

impl Task {
    pub fn execute(self) {
        (self.op)();
    }
}

enum WorkerThreadStatus {
    Complete(u16),
}

pub struct BoundedThreadPool {
    api_tx: Sender<ThreadPoolOperation>,
    manager_handle: JoinHandle<()>,
}

impl BoundedThreadPool {
    pub fn new(worker_count: u16) -> Self {
        let (api_tx, api_rx) = crossbeam::channel::unbounded::<ThreadPoolOperation>();

        Self {
            api_tx,
            manager_handle: BoundedThreadPoolManager::new(worker_count, api_rx).manage(),
        }
    }

    pub fn submit<F>(&self, task: F)
    where
        F: Fn() + Send + 'static,
    {
        let task = Task { op: Box::new(task) };
        self.api_tx.send(ThreadPoolOperation::Submit(task));
    }

    pub fn shutdown(self, hard: bool) {
        self.api_tx.send(ThreadPoolOperation::Close(hard));
        self.manager_handle.join();
    }
}

enum ThreadPoolOperation {
    Submit(Task),
    Close(bool),
}

pub struct BoundedThreadPoolManager {
    workers: HashMap<u16, WorkerThread>,
    ready: VecDeque<u16>,
    task_queue: VecDeque<Task>,
    status_rx: Receiver<WorkerThreadStatus>,
    api_rx: Receiver<ThreadPoolOperation>,
}

impl BoundedThreadPoolManager {
    fn new(worker_count: u16, api_rx: Receiver<ThreadPoolOperation>) -> Self {
        let (status_tx, status_rx) = crossbeam::channel::unbounded::<WorkerThreadStatus>();

        Self {
            workers: (0..worker_count)
                .map(|id| (id, WorkerThread::new(id, status_tx.clone())))
                .collect(),
            ready: VecDeque::from_iter(0..worker_count),
            task_queue: VecDeque::with_capacity(worker_count as usize),
            status_rx,
            api_rx,
        }
    }

    fn manage(mut self) -> JoinHandle<()> {
        std::thread::spawn(move || loop {
            select! {
                recv(self.status_rx) -> msg => match msg {
                    Ok(status) => match status {
                        WorkerThreadStatus::Complete(id) => self.ready.push_back(id),
                    }
                    Err(_) => todo!(),
                },
                recv(self.api_rx) -> msg => match msg {
                    Ok(operation) => match operation {
                        ThreadPoolOperation::Submit(task) => self.submit(task),
                        ThreadPoolOperation::Close(_) => break // todo kill child processes
                    }
                    Err(_) => todo!(),
                },
                default => if let Some(task) = self.task_queue.pop_front() { self.submit(task); }
            }
        })
    }

    fn submit(&mut self, task: Task) {
        match self.ready.pop_front() {
            Some(worker) => {
                let thread = self
                    .workers
                    .get(&worker)
                    .expect(&format!("thread id {} not found in pool", worker));
                thread.queue_task(task);
            }
            None => todo!(),
        }
    }
}

pub fn main() {
    let pool = BoundedThreadPool::new(4);

    pool.submit(move || {
        sleep(Duration::from_secs(2));
        println!("world")
    });
    pool.submit(move || println!("hello"));

    sleep(Duration::from_secs(3));
    pool.shutdown(true);
}

struct WorkerThread {
    id: u16,
    thread: JoinHandle<()>,
    tx_task: Sender<Task>,
    tx_status: Sender<WorkerThreadStatus>,
}

impl WorkerThread {
    fn new(id: u16, tx_status: Sender<WorkerThreadStatus>) -> Self {
        let (tx_task, rx) = crossbeam::channel::unbounded::<Task>();
        let tx_status_clone = tx_status.clone();

        let thread = std::thread::spawn(move || loop {
            match rx.recv() {
                Ok(task) => {
                    task.execute();
                    tx_status_clone.send(WorkerThreadStatus::Complete(id));
                }
                Err(_) => break,
            }
        });

        WorkerThread {
            id,
            thread,
            tx_task,
            tx_status,
        }
    }

    pub fn queue_task(&self, task: Task) -> Result<(), SendError<Task>> {
        self.tx_task.send(task)
    }
}

#[cfg(test)]
mod test {
    use std::{thread::sleep, time::Duration};

    use super::*;

    #[test]
    fn submit_task() {}
}
