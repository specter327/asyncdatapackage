"""
AsyncDataPackage

Generic async message framing library.

Features:
- asyncio native
- JSON messages
- Delimiter-based framing
- Bounded queue
- Configurable backpressure
- Graceful shutdown
- Stream agnostic
- Reactive callbacks

Author:
    Specter327

License:
    MIT
"""

from __future__ import annotations

import asyncio
import json
import logging

from typing import (
    Awaitable,
    Callable,
    Optional
)

logger = logging.getLogger(__name__)


class AsyncDataPackageError(Exception):
    pass


class AsyncDataPackageStateError(
    AsyncDataPackageError
):
    pass


class AsyncDataPackage:

    DEFAULT_DELIMITER = (
        b"\x01\x02\x03\x01\x01\x01"
    )

    def __init__(
        self,
        *,
        write_function: Callable[
            ...,
            Awaitable[bool]
        ],
        read_function: Callable[
            ...,
            Awaitable[bytes]
        ],
        read_arguments: tuple = (),
        read_keyword_arguments: Optional[
            dict
        ] = None,
        write_arguments: tuple = (),
        write_keyword_arguments: Optional[
            dict
        ] = None,
        delimiter: bytes = (
            DEFAULT_DELIMITER
        ),
        max_queue_size: int = 1000,
        max_buffer_size: int = (
            10 * 1024 * 1024
        ),
        backpressure_mode: str = (
            "block"
        )
    ):

        if max_queue_size < 1:

            raise ValueError(
                "max_queue_size "
                "must be >= 1"
            )

        if max_buffer_size < 1024:

            raise ValueError(
                "max_buffer_size "
                "too small"
            )

        if backpressure_mode not in (
            "block",
            "drop",
            "drop_oldest"
        ):

            raise ValueError(
                f"Invalid "
                f"backpressure mode: "
                f"{backpressure_mode}"
            )

        self._write_function = (
            write_function
        )

        self._read_function = (
            read_function
        )

        self._read_arguments = (
            read_arguments
        )

        self._read_kwargs = (
            read_keyword_arguments
            or {}
        )

        self._write_arguments = (
            write_arguments
        )

        self._write_kwargs = (
            write_keyword_arguments
            or {}
        )

        self._delimiter = delimiter

        self._queue = (
            asyncio.Queue(
                maxsize=max_queue_size
            )
        )

        self._max_buffer_size = (
            max_buffer_size
        )

        self._backpressure_mode = (
            backpressure_mode
        )

        self._buffer = bytearray()

        self._running = False

        self._reader_task: Optional[
            asyncio.Task
        ] = None

        self._datapackage_callbacks = []

    # -------------------------------------------------
    # Lifecycle
    # -------------------------------------------------

    async def start(
        self
    ) -> bool:

        if self._running:
            return True

        self._running = True

        self._reader_task = (
            asyncio.create_task(
                self._reader_loop(),
                name=(
                    "AsyncDataPackageReader"
                )
            )
        )

        return True

    async def stop(
        self
    ) -> bool:

        self._running = False

        if self._reader_task:

            self._reader_task.cancel()

            try:

                await (
                    self._reader_task
                )

            except (
                asyncio.CancelledError
            ):
                pass

        return True

    def is_running(
        self
    ) -> bool:

        return self._running

    # -------------------------------------------------
    # Reader
    # -------------------------------------------------

    async def _reader_loop(
        self
    ):

        while self._running:

            try:

                chunk = await (
                    self._read_function(
                        *self._read_arguments,
                        **self._read_kwargs
                    )
                )

                if not chunk:

                    await asyncio.sleep(
                        0.01
                    )

                    continue

                self._buffer.extend(
                    chunk
                )

                if (
                    len(self._buffer)
                    >
                    self._max_buffer_size
                ):

                    logger.warning(
                        "Reception buffer "
                        "exceeded maximum "
                        "size. Clearing."
                    )

                    self._buffer.clear()

                    continue

                await (
                    self._extract_packets()
                )

            except (
                asyncio.CancelledError
            ):
                break

            except Exception:

                logger.exception(
                    "Reader loop "
                    "exception"
                )

                await asyncio.sleep(
                    0.1
                )

    async def _extract_packets(
        self
    ):

        while True:

            position = (
                self._buffer.find(
                    self._delimiter
                )
            )

            if position == -1:
                break

            packet_bytes = bytes(
                self._buffer[
                    :position
                ]
            )

            del self._buffer[
                : position
                + len(
                    self._delimiter
                )
            ]

            if not packet_bytes:
                continue

            await (
                self._process_packet(
                    packet_bytes
                )
            )

    async def _process_packet(
        self,
        packet_bytes: bytes
    ):

        try:

            packet = json.loads(
                packet_bytes.decode(
                    "utf-8"
                )
            )

        except Exception:

            logger.warning(
                "Invalid packet "
                "received"
            )

            return

        if (
            self._backpressure_mode
            == "block"
        ):

            await self._queue.put(
                packet
            )

        elif (
            self._backpressure_mode
            == "drop"
        ):

            if self._queue.full():

                logger.warning(
                    "Queue full. "
                    "Dropping packet."
                )

            else:

                self._queue.put_nowait(
                    packet
                )

        elif (
            self._backpressure_mode
            == "drop_oldest"
        ):

            if self._queue.full():

                try:

                    self._queue.get_nowait()

                except (
                    asyncio.QueueEmpty
                ):
                    pass

            self._queue.put_nowait(
                packet
            )

        await (
            self._fire_datapackage_callbacks(
                packet
            )
        )

    async def _safe_callback_call(
        self,
        callback,
        packet
    ):

        try:

            await callback(
                packet
            )

        except Exception:

            logger.exception(
                "Datapackage "
                "callback failed"
            )

    async def _fire_datapackage_callbacks(
        self,
        packet: dict
    ):

        callbacks = tuple(
            self._datapackage_callbacks
        )

        for callback in callbacks:

            try:

                if (
                    asyncio
                    .iscoroutinefunction(
                        callback
                    )
                ):

                    asyncio.create_task(
                        self._safe_callback_call(
                            callback,
                            packet
                        )
                    )

                else:

                    callback(packet)

            except Exception:

                logger.exception(
                    "Datapackage "
                    "callback failed"
                )

    # -------------------------------------------------
    # Send
    # -------------------------------------------------

    async def send_datapackage(
        self,
        datapackage: dict
    ) -> bool:

        try:

            payload = (
                json.dumps(
                    datapackage,
                    separators=(
                        ",",
                        ":"
                    ),
                    ensure_ascii=False
                ).encode(
                    "utf-8"
                )
                +
                self._delimiter
            )

        except Exception:

            logger.exception(
                "Serialization error"
            )

            return False

        try:

            return bool(
                await (
                    self._write_function(
                        *self._write_arguments,
                        payload,
                        **self._write_kwargs
                    )
                )
            )

        except Exception:

            logger.exception(
                "Write error"
            )

            return False


    # -------------------------------------------------
    # Receive
    # -------------------------------------------------

    async def receive_datapackage(
        self,
        timeout: Optional[
            float
        ] = None
    ):

        try:

            if timeout is None:

                return await (
                    self._queue.get()
                )

            return await (
                asyncio.wait_for(
                    self._queue.get(),
                    timeout
                )
            )

        except (
            asyncio.TimeoutError
        ):

            return None

    # -------------------------------------------------
    # Dynamic configuration
    # -------------------------------------------------

    def update_read_parameters(
        self,
        *args,
        **kwargs
    ):

        self._read_arguments = args
        self._read_kwargs = kwargs

    def update_write_parameters(
        self,
        *args,
        **kwargs
    ):

        self._write_arguments = args
        self._write_kwargs = kwargs

    # -------------------------------------------------
    # Utilities
    # -------------------------------------------------

    def pending_packages(
        self
    ) -> int:

        return (
            self._queue.qsize()
        )

    async def drain_queue(
        self
    ):

        packets = []

        while True:

            try:

                packets.append(
                    self._queue.get_nowait()
                )

            except (
                asyncio.QueueEmpty
            ):
                break

        return packets

    # -------------------------------------------------
    # Callbacks
    # -------------------------------------------------

    def on_datapackage_receive(
        self,
        callback
    ):

        if not callable(
            callback
        ):

            raise TypeError(
                "callback must "
                "be callable"
            )

        if callback not in (
            self._datapackage_callbacks
        ):

            self._datapackage_callbacks.append(
                callback
            )

        return True

    def remove_datapackage_callback(
        self,
        callback
    ):

        try:

            self._datapackage_callbacks.remove(
                callback
            )

            return True

        except ValueError:

            return False

    def clear_datapackage_callbacks(
        self
    ):

        self._datapackage_callbacks.clear()

        return True