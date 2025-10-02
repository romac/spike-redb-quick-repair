Benchmark comparing `quick_repair(true)` vs `quick_repair(false)` impact on write performance using a `redb` database.

```
$ cargo run --release -- --target-size-gb TARGET_SIZE
```

- `TARGET_SIZE`: Amount of data to insert into the database (in GiB)
