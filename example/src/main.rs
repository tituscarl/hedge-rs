use crossbeam_channel::unbounded;
use log::*;
use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

extern crate num_cpus;

fn channels() {
    let (s, r) = unbounded();
    let ch = Arc::new(Mutex::new(Vec::new()));
    let cpus = num_cpus::get();
    info!("cpus={}", cpus);

    for i in 0..cpus {
        info!("cpu{}", i);
        let ch = ch.clone();
        {
            let mut c = ch.lock().unwrap();
            c.push(r.clone());
        }
    }

    for i in 0..cpus {
        let ch = ch.clone();
        thread::spawn(move || {
            info!("start thread-{i}");
            loop {
                let c = ch.lock();
                if c.is_err() {
                    error!("{i}: lock failed");
                    break;
                }

                let cv = c.unwrap();
                match cv[i].recv() {
                    Ok(v) => {
                        info!("t{i}: {:?}", v);
                    }
                    Err(_) => {}
                }

                drop(cv);
                thread::sleep(Duration::from_millis(50));
            }
        });
    }

    thread::sleep(Duration::from_secs(5));
    info!("start send");

    s.send(10).unwrap();
    s.send(20).unwrap();
    s.send(30).unwrap();
    s.send(40).unwrap();
    s.send(50).unwrap();
    s.send(60).unwrap();
    s.send(70).unwrap();
    s.send(80).unwrap();
    s.send(10).unwrap();
    s.send(20).unwrap();
    s.send(30).unwrap();
    s.send(40).unwrap();
    s.send(50).unwrap();
    s.send(60).unwrap();
    s.send(70).unwrap();
    s.send(80).unwrap();

    thread::sleep(Duration::from_secs(5));
}

fn main() {
    env_logger::init();
    channels();

    // let q: ArrayQueue<Option<i32>> = ArrayQueue::new(8);
    // let cpus = num_cpus::get();
    // info!("cpus={}", cpus);

    // for i in 0..cpus {
    //     thread::spawn(move || {
    //         info!("start thread-{i}");
    //         loop {
    //             let v = q.pop();
    //             info!("t{i}: {:?}", v);
    //             thread::sleep(Duration::from_millis(1000));
    //         }
    //     });
    // }

    // thread::sleep(Duration::from_secs(5));
    // info!("start send");

    // s.send(10).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(20).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(30).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(40).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(50).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(60).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(70).unwrap();
    // thread::sleep(Duration::from_millis(500));
    // s.send(80).unwrap();

    thread::sleep(Duration::from_secs(2));
}
