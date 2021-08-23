<h1 align="center">async-sub</h1>
<div align="center">
  <strong>
    Async & reactive subscription model to keep multiple async tasks / threads
    partially synchronized.
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

```rust
async fn main() {
    let mut observable = Observable::new(0);
    let mut subscription = observable.subscribe();

    observable.publish(1);

    assert_eq!(subscription.wait().await, 1);
}
```
