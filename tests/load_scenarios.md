## Performance Benchmark

```bash
wrk -t4 -c128 -d30s http://127.0.0.1:8888/
Running 30s test @ http://127.0.0.1:8888/
  4 threads and 128 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    12.28ms   13.03ms 240.07ms   95.92%
    Req/Sec     3.02k   287.23     3.73k    69.75%
  360488 requests in 30.02s, 44.00MB read
Requests/sec:  12007.92
Transfer/sec:      1.47MB
```