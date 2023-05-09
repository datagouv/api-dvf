# API for DVF v2

- API with stats on each geographical level
- API of all mutations of a cadastral section

```
gunicorn api_aio:app_factory --bind 0.0.0.0:3030 --worker-class aiohttp.GunicornWebWorker --workers 4
```

