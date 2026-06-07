import asyncio
import json
import traceback

from typing import Optional


class AsyncDataPackage:

    PACKAGE_DELIMITER = b"\x01\x02\x03\x01\x01\x01"

    def __init__(
        self,
        write_function,
        read_function,
        read_arguments=None,
        read_keyword_arguments=None,
        write_arguments=None,
        write_keyword_arguments=None
    ):

        self._write_function = write_function
        self._read_function = read_function

        self._read_arguments = (
            read_arguments
            if read_arguments
            else ()
        )

        self._read_keyword_arguments = (
            read_keyword_arguments
            if read_keyword_arguments
            else {}
        )

        self._write_arguments = (
            write_arguments
            if write_arguments
            else ()
        )

        self._write_keyword_arguments = (
            write_keyword_arguments
            if write_keyword_arguments
            else {}
        )

        self._package_queue = asyncio.Queue()

        self._reception_buffer = b""

        self._running = False
        self._reader_task = None

        self._lock = asyncio.Lock()

    # =====================================================
    # START / STOP
    # =====================================================

    async def start(self):

        if self._running:
            return True

        self._running = True

        self._reader_task = asyncio.create_task(
            self._reader_loop()
        )

        return True

    async def stop(self):

        self._running = False

        if self._reader_task:

            self._reader_task.cancel()

            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        return True

    # =====================================================
    # READER
    # =====================================================

    async def _reader_loop(self):

        while self._running:

            try:

                async with self._lock:

                    chunk = await self._read_function(
                        *self._read_arguments,
                        **self._read_keyword_arguments
                    )

                if not chunk:

                    await asyncio.sleep(
                        0.01
                    )

                    continue

                self._reception_buffer += chunk

                while (
                    self.PACKAGE_DELIMITER
                    in
                    self._reception_buffer
                ):

                    raw_packet, self._reception_buffer = (
                        self._reception_buffer.split(
                            self.PACKAGE_DELIMITER,
                            1
                        )
                    )

                    if raw_packet:

                        await self._process_packet(
                            raw_packet
                        )

            except asyncio.CancelledError:
                return

            except Exception:
                traceback.print_exc()

                await asyncio.sleep(
                    0.05
                )

    async def _process_packet(
        self,
        raw_packet: bytes
    ):

        try:

            packet = json.loads(
                raw_packet.decode(
                    "utf-8"
                )
            )

            await self._package_queue.put(
                packet
            )

        except Exception:
            traceback.print_exc()

    # =====================================================
    # CONFIG
    # =====================================================

    async def update_reception_parameters(
        self,
        *args,
        **kwargs
    ):

        async with self._lock:

            self._read_arguments = args
            self._read_keyword_arguments = kwargs

        return True

    async def update_send_parameters(
        self,
        *args,
        **kwargs
    ):

        async with self._lock:

            self._write_arguments = args
            self._write_keyword_arguments = kwargs

        return True

    # =====================================================
    # SEND
    # =====================================================

    async def send_datapackage(
        self,
        datapackage: dict
    ) -> bool:

        try:

            payload = (
                json.dumps(
                    datapackage
                ).encode(
                    "utf-8"
                )
                +
                self.PACKAGE_DELIMITER
            )

            return await self._write_function(
                payload,
                *self._write_arguments,
                **self._write_keyword_arguments
            )

        except Exception:

            traceback.print_exc()

            return False

    # =====================================================
    # RECEIVE
    # =====================================================

    async def receive_datapackage(
        self,
        timeout: Optional[float] = None
    ):

        try:

            if timeout is None:

                return await self._package_queue.get()

            return await asyncio.wait_for(
                self._package_queue.get(),
                timeout
            )

        except asyncio.TimeoutError:
            return None

    # =====================================================
    # QUERIES
    # =====================================================

    def pending_packages(self):

        return self._package_queue.qsize()