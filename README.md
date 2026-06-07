# AsyncDataPackage

AsyncDataPackage is a lightweight asyncio-based framing layer that converts arbitrary byte streams into structured Python dictionaries.

It is transport agnostic and can operate on:

- TCP
- SSH
- WebSockets
- Bluetooth
- Serial Ports
- UDP
- Custom transports

The transport only needs to provide:

```python
await send(bytes)
await receive(...)
```

---

## Installation

```bash
pip install asyncdatapackage
```

---

## Quick Example

```python
from asyncdatapackage import AsyncDataPackage
```

```python
datapackage = AsyncDataPackage(
    write_function=my_send,
    read_function=my_receive
)

await datapackage.start()
```

Send:

```python
await datapackage.send_datapackage(
    {
        "type": "DATA",
        "message": "hello"
    }
)
```

Receive:

```python
packet = await datapackage.receive_datapackage()
```

Result:

```python
{
    "type": "DATA",
    "message": "hello"
}
```

---

## Features

- asyncio-native
- Dynamic parameter updates
- FIFO packet queue
- Automatic frame reconstruction
- Fragmentation tolerant
- Transport independent

---

## License

MIT