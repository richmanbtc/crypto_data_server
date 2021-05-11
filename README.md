

dockerで起動する場合。

docker-compose up -d

## developer

```bash
pyenv exec pipenv run python src/server.py
```


```bash
pyenv exec pipenv run python src/server.py --start_time 1617202800
```

test request

```bash
curl -v "http://localhost:5000/ohlcv.parquet?exchange=bybit&markets=BTCUSD&interval=3600&end_time=1617289200" | wc
```

test

```bash
pyenv exec pipenv run python -m unittest tests/test_*
```

