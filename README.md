# Gossip Glomers

Instructions: https://fly.io/dist-sys

Maelstrom: https://github.com/jepsen-io/maelstrom/tree/main

## Echo

```sh
java -jar maelstrom.jar test -w echo  --bin target/debug/echo --node-count 1 --time-limit 10
```

## Unique IDs

```sh
java -jar maelstrom.jar test -w unique-ids --bin target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

## Broadcast

```sh
java -jar maelstrom.jar test -w broadcast --bin target/debug/broadcast --node-count 5 --time-limit 20 --rate 10
```

## Grow only counter

```sh
java -jar maelstrom.jar test -w g-counter --bin target/debug/g-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```
