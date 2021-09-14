<h1 align="center">async-sub</h1>
<div align="center">
  <strong>
    Async & reactive synchronization model to keep multiple async tasks /
    threads partially synchronized.
  </strong>
</div>
<br />
<div align="center">
  <a href="https://crates.io/crates/async-sub">
    <img src="https://img.shields.io/crates/v/async-sub.svg?style=flat-square"
    alt="crates.io version" />
  </a>
  <a href="https://docs.rs/async-sub">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
</div>

## Examples

### Simple Forking

```rust
use async_sub::Observable;

#[async_std::main]
async fn main() {
    let mut observable = Observable::new(0);
    let mut fork = observable.fork();

    observable.publish(1);

    assert_eq!(fork.wait().await, 1);
}
```

### Notifying A Task

```rust
use async_std::task::{sleep, spawn};
use async_sub::Observable;

#[async_std::main]
async fn main() {
    let mut observable = Observable::new(0);
    let mut fork = observable.fork();

    let task = spawn(async move {
        loop {
            let update = fork.next().await;
            println!("task received update {}", update);

            if update >= 3 {
                break;
            }
        }
    });

    observable.publish(1);
    sleep(std::time::Duration::from_millis(100)).await;
    observable.publish(2);
    sleep(std::time::Duration::from_millis(100)).await;
    observable.publish(3);

    task.await;
}
```

### Execution Control

You may mimic the behavior of a mutex but with an observable you can kick of
many asynchronous tasks if the value changes. We'll just use a bool
observable, which we publish only once.

```rust
use async_std::task::{sleep, spawn};
use async_sub::Observable;
use futures::join;

#[async_std::main]
async fn main() {
    let mut execute = Observable::new(false);
    let mut execute_fork_one = execute.fork();
    let mut execute_fork_two = execute.fork();

    let task_one = spawn(async move {
        println!("task one started");
        execute_fork_one.next().await;
        println!("task one ran");
    });

    let task_two = spawn(async move {
        println!("task two started");
        execute_fork_two.next().await;
        println!("task two ran");
    });

    join!(
        task_one,
        task_two,
        spawn(async move {
            println!("main task started");

            // run some fancy business logic
            sleep(std::time::Duration::from_millis(100)).await;
            // then release our tasks to do stuff when we are done
            execute.publish(true);

            println!("main task ran");
        })
    );
}
```

You could argue and say that you may aswell just spawn the tasks in the moment
you want to kick of something - thats true and the better solution if you just
want sub tasks. _But if you want to notify a completly different part of your
program this becomes hard._ Or for example if you want to run a task in
half, wait for something the other task did and then resume.
