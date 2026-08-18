import binascii
from typing import Any, Optional

from scalecodec import ScaleBytes, GenericMetadataVersioned
from scalecodec.base import ScaleDecoder, RuntimeConfigurationObject, ScaleType
from scalecodec.utils.ss58 import ss58_decode
from async_substrate_interface.errors import StorageFunctionNotFound
from async_substrate_interface.utils.hasher import (
    blake2_256,
    two_x64_concat,
    xxh128,
    blake2_128,
    blake2_128_concat,
    identity,
)

from typing import Self

# Whole-batch storage-key hashing (hex parse + BLAKE2b + concat per key in
# one C loop).
from scalecodec.utils._ss58 import blake2_128_concat_batch

# Single source of truth mapping a metadata hasher name to its implementation.
# `None`/empty hasher defaults to "Twox128" (matches substrate behaviour).
PARAM_HASHERS = {
    "Blake2_256": blake2_256,
    "Blake2_128": blake2_128,
    "Blake2_128Concat": blake2_128_concat,
    "Twox128": xxh128,
    "Twox64Concat": two_x64_concat,
    "Identity": identity,
}


class StorageKey:
    """
    A StorageKey instance is a representation of a single state entry.

    Substrate uses a simple key-value data store implemented as a database-backed, modified Merkle tree.
    All of Substrate's higher-level storage abstractions are built on top of this simple key-value store.
    """

    __slots__ = (
        "pallet",
        "storage_function",
        "params",
        "_params_encoded",
        "data",
        "metadata",
        "runtime_config",
        "value_scale_type",
        "metadata_storage_function",
    )

    _params_encoded: list[Any]

    @property
    def params_encoded(self) -> list[Any]:
        """Encoded params as ``ScaleBytes``, one per param.

        The batch builder stores raw ``bytes`` (or the source ``"0x..."`` hex
        strings) for byte-transparent params; they are wrapped in
        ``ScaleBytes`` lazily on first access.
        """
        pe = self._params_encoded
        if any(type(x) in (bytes, str) for x in pe):
            pe = [
                ScaleBytes(x)
                if type(x) is bytes
                else (ScaleBytes(bytes.fromhex(x[2:])) if type(x) is str else x)
                for x in pe
            ]
            self._params_encoded = pe
        return pe

    @params_encoded.setter
    def params_encoded(self, value: list[Any]) -> None:
        self._params_encoded = value

    def __init__(
        self,
        pallet: Optional[str],
        storage_function: Optional[str],
        params: Optional[list],
        data: Optional[bytes],
        value_scale_type: Optional[str],
        metadata: GenericMetadataVersioned,
        runtime_config: RuntimeConfigurationObject,
    ):
        self.pallet = pallet
        self.storage_function = storage_function
        self.params = params
        self._params_encoded: list[Any] = []
        self.data = data
        self.metadata = metadata
        self.runtime_config = runtime_config
        self.value_scale_type = value_scale_type
        self.metadata_storage_function: Optional[Any] = None

    @classmethod
    def create_from_data(
        cls,
        data: bytes,
        runtime_config: RuntimeConfigurationObject,
        metadata: GenericMetadataVersioned,
        value_scale_type: Optional[str] = None,
        pallet: Optional[str] = None,
        storage_function: Optional[str] = None,
    ) -> Self:
        """
        Create a StorageKey instance providing raw storage key bytes

        Args:
            data: bytes representation of the storage key
            runtime_config: RuntimeConfigurationObject
            metadata: GenericMetadataVersioned
            value_scale_type: type string of to decode result data
            pallet: name of pallet
            storage_function: name of storage function

        Returns:
            StorageKey
        """
        if not value_scale_type and pallet and storage_function:
            metadata_pallet = metadata.get_metadata_pallet(pallet)

            if not metadata_pallet:
                raise StorageFunctionNotFound(f'Pallet "{pallet}" not found')

            storage_item = metadata_pallet.get_storage_function(storage_function)

            if not storage_item:
                raise StorageFunctionNotFound(
                    f'Storage function "{pallet}.{storage_function}" not found'
                )

            # Process specific type of storage function
            value_scale_type = storage_item.get_value_type_string()

        return cls(
            pallet=None,
            storage_function=None,
            params=None,
            data=data,
            metadata=metadata,
            value_scale_type=value_scale_type,
            runtime_config=runtime_config,
        )

    @classmethod
    def prepared(
        cls,
        pallet: str,
        storage_function: str,
        runtime_config: RuntimeConfigurationObject,
        metadata: GenericMetadataVersioned,
    ) -> tuple:
        """
        Everything that is constant per storage function, resolved once and
        cached on the metadata object: ``(metadata_storage_function,
        value_scale_type, param_types, hasher_fns, prefix, scale_objects,
        batch_cls)``.

        Metadata resolution (pallet scan, storage-entry scan), the pallet/item
        prefix hash, and the param encoder objects dominate single storage-key
        construction; caching them makes per-key work just the param encode +
        param hash.

        ``batch_cls`` is a StorageKey subclass whose batch-constant attributes
        (pallet, storage function, metadata, runtime config, value scale type,
        metadata storage function) are plain class attributes shadowing the
        base slots, so batch construction only writes the three per-key slots
        (``params``, ``data``, ``_params_encoded``).

        Raises StorageFunctionNotFound when the pallet or storage function
        does not exist.
        """
        cache = metadata.__dict__.setdefault("_asi_storage_fn_cache", {})
        key = (pallet, storage_function)
        prep = cache.get(key)
        if prep is not None:
            return prep

        metadata_pallet = metadata.get_metadata_pallet(pallet)
        if not metadata_pallet:
            raise StorageFunctionNotFound(f'Pallet "{pallet}" not found')

        metadata_storage_function = metadata_pallet.get_storage_function(
            storage_function
        )
        if not metadata_storage_function:
            raise StorageFunctionNotFound(
                f'Storage function "{pallet}.{storage_function}" not found'
            )

        value_scale_type = metadata_storage_function.get_value_type_string()
        param_types = metadata_storage_function.get_params_type_string()
        hashers = metadata_storage_function.get_param_hashers()

        # Immutable bytes: per-key accumulation does `storage_hash = prefix`
        # then `+=`, which must allocate rather than mutate this shared prefix.
        prefix = bytes(
            xxh128(metadata_pallet.value["storage"]["prefix"].encode())
            + xxh128(storage_function.encode())
        )

        scale_objects = [
            runtime_config.create_scale_object(type_string=type_string)
            for type_string in param_types
        ]
        hasher_fns = []
        for idx in range(len(param_types)):
            param_hasher = hashers[idx] if idx < len(hashers) else None
            try:
                hasher_fns.append(PARAM_HASHERS[param_hasher or "Twox128"])
            except KeyError:
                raise ValueError('Unknown storage hasher "{}"'.format(param_hasher))

        batch_cls = type(
            f"_Batch_{pallet}_{storage_function}",
            (cls,),
            {
                "__slots__": (),
                "pallet": pallet,
                "storage_function": storage_function,
                "metadata": metadata,
                "runtime_config": runtime_config,
                "value_scale_type": value_scale_type,
                "metadata_storage_function": metadata_storage_function,
            },
        )

        prep = (
            metadata_storage_function,
            value_scale_type,
            param_types,
            hasher_fns,
            prefix,
            scale_objects,
            batch_cls,
        )
        cache[key] = prep
        return prep

    @classmethod
    def create_from_storage_function(
        cls,
        pallet: str,
        storage_function: str,
        params: list,
        runtime_config: RuntimeConfigurationObject,
        metadata: GenericMetadataVersioned,
    ) -> Self:
        """
        Create a StorageKey instance providing storage function details

        Args:
            pallet: name of pallet
            storage_function: name of storage function
            params: Optional list of parameters in case of a Mapped storage function
            runtime_config: RuntimeConfigurationObject
            metadata: GenericMetadataVersioned

        Returns:
            StorageKey
        """
        (
            metadata_storage_function,
            value_scale_type,
            param_types,
            hasher_fns,
            prefix,
            scale_objects,
            _,
        ) = cls.prepared(pallet, storage_function, runtime_config, metadata)

        ss58_format = runtime_config.ss58_format
        storage_hash = prefix
        params_encoded: list[Any] = []
        for idx, param in enumerate(params or []):
            if idx >= len(hasher_fns):
                raise ValueError(f"No hasher found for param #{idx + 1}")
            if type(param) is ScaleBytes:
                # Already encoded
                encoded = param
                params_key = param.data
            else:
                param = cls._convert_storage_parameter(
                    param_types[idx], param, ss58_format
                )
                encoded = scale_objects[idx].encode(param)
                params_key = encoded.data
            params_encoded.append(encoded)
            storage_hash += hasher_fns[idx](params_key)

        storage_key_obj = cls(
            pallet=pallet,
            storage_function=storage_function,
            params=params,
            data=None,
            runtime_config=runtime_config,
            metadata=metadata,
            value_scale_type=value_scale_type,
        )
        storage_key_obj.data = storage_hash
        storage_key_obj.metadata_storage_function = metadata_storage_function
        storage_key_obj.params_encoded = params_encoded

        return storage_key_obj

    @classmethod
    def create_from_storage_function_batch(
        cls,
        pallet: str,
        storage_function: str,
        params_list: list[list],
        runtime_config: RuntimeConfigurationObject,
        metadata: GenericMetadataVersioned,
    ) -> list[Self]:
        """
        Create many StorageKey instances for the same pallet/storage_function in
        one pass, one per entry in ``params_list``.

        This is much faster than calling :meth:`create_from_storage_function`
        in a loop: everything that is constant across the keys (metadata
        resolution, the pallet/storage-function prefix hash, and the scale
        objects used to encode params) is computed once and reused. For large
        batches (e.g. 100k keys) this is ~30x faster while producing
        byte-identical keys.

        Args:
            pallet: name of pallet
            storage_function: name of storage function
            params_list: list of parameter lists, one per storage key to create
            runtime_config: RuntimeConfigurationObject
            metadata: GenericMetadataVersioned

        Returns:
            list of StorageKey, in the same order as ``params_list``
        """
        # --- Everything constant across the batch, from the shared cache. ---
        (
            metadata_storage_function,
            value_scale_type,
            param_types,
            hasher_fns,
            prefix,
            scale_objects,
            batch_cls,
        ) = cls.prepared(pallet, storage_function, runtime_config, metadata)

        ss58_format = runtime_config.ss58_format

        # C fast path for the dominant shape: a single Blake2_128Concat
        # parameter passed as "0x..." hex (or raw bytes) whose SCALE encoding
        # is byte-transparent (AccountId, H256, ...). One representative param
        # is round-tripped through the real encoder to prove transparency and
        # fix the raw length; the whole batch is then hashed in one C call.
        # Any nonconforming entry (ss58 string, wrong length, int, ScaleBytes)
        # raises ValueError and the batch falls back to the generic loop.
        if (
            len(param_types) == 1
            and hasher_fns[0] is blake2_128_concat
            and params_list
            and len(params_list[0]) == 1
        ):
            probe = params_list[0][0]
            if type(probe) is str and probe[:2] == "0x":
                encoded0 = scale_objects[0].encode(
                    cls._convert_storage_parameter(param_types[0], probe, ss58_format)
                )
                enc_bytes = bytes(encoded0.data)
                if (
                    len(probe) == 2 + 2 * len(enc_bytes)
                    and bytes.fromhex(probe[2:]) == enc_bytes
                ):
                    try:
                        simple_params = [p[0] for p in params_list if len(p) == 1]
                        if len(simple_params) != len(params_list):
                            key_datas = None
                        else:
                            key_datas = blake2_128_concat_batch(
                                prefix, simple_params, len(enc_bytes)
                            )
                    except ValueError:
                        key_datas = None
                    if key_datas is not None:
                        new_ = batch_cls.__new__
                        storage_keys_: list[Self] = []
                        append_ = storage_keys_.append
                        for params, data in zip(params_list, key_datas):
                            obj = new_(batch_cls)
                            obj.params = params
                            obj.data = data
                            # hex str / bytes params; the params_encoded
                            # property wraps them into ScaleBytes lazily.
                            obj._params_encoded = params
                            append_(obj)
                        return storage_keys_

        # Per-position identity-encode probe: many storage params (AccountId,
        # H256, [u8; N]) are passed as "0x..." hex strings (or raw bytes) whose
        # SCALE encoding is exactly those bytes. The first key that goes
        # through the full encoder at a position establishes whether the type
        # is byte-transparent (encoded bytes == the input bytes); after that,
        # same-length inputs at that position skip the SCALE encoder entirely.
        # Value-dependent encodings (Vec<u8> length prefix, enums, compacts)
        # fail the probe and always take the full encoder.
        #   None: unprobed; 0: not byte-transparent; >0: raw byte length.
        identity_raw_len: list[Optional[int]] = [None] * len(param_types)

        convert = cls._convert_storage_parameter
        new = batch_cls.__new__
        storage_keys: list[Self] = []
        append = storage_keys.append
        for params in params_list:
            storage_hash = prefix
            params_encoded: list[Any] = []
            for idx, param in enumerate(params):
                if type(param) is ScaleBytes:
                    # Already encoded
                    encoded = param
                    params_key = param.data
                else:
                    raw_len = identity_raw_len[idx]
                    if raw_len:
                        if type(param) is str:
                            if len(param) == 2 + 2 * raw_len and param[:2] == "0x":
                                params_key = bytes.fromhex(param[2:])
                            else:
                                params_key = None
                        elif type(param) is bytes and len(param) == raw_len:
                            params_key = param
                        else:
                            params_key = None
                        if params_key is not None:
                            # Raw bytes; the params_encoded property wraps
                            # these in ScaleBytes lazily on access.
                            encoded = params_key
                        else:
                            param = convert(param_types[idx], param, ss58_format)
                            encoded = scale_objects[idx].encode(param)
                            params_key = encoded.data
                    else:
                        param = convert(param_types[idx], param, ss58_format)
                        encoded = scale_objects[idx].encode(param)
                        params_key = encoded.data
                        if raw_len is None:
                            identity_raw_len[idx] = (
                                len(params_key)
                                if (
                                    type(param) is str
                                    and param[:2] == "0x"
                                    and len(param) == 2 + 2 * len(params_key)
                                    and bytes.fromhex(param[2:]) == params_key
                                )
                                else 0
                            )
                params_encoded.append(encoded)
                storage_hash += hasher_fns[idx](params_key)

            # Only the per-key slots are written; the batch-constant
            # attributes live on batch_cls as class attributes.
            storage_key_obj = new(batch_cls)
            storage_key_obj.params = params
            storage_key_obj._params_encoded = params_encoded
            # Mirror generate(): the hash is assigned onto self.data directly.
            storage_key_obj.data = storage_hash
            append(storage_key_obj)

        return storage_keys

    @staticmethod
    def _convert_storage_parameter(
        scale_type: str, value: Any, ss58_format: Optional[int]
    ):
        if type(value) is bytes:
            value = f"0x{value.hex()}"

        if scale_type == "AccountId":
            if value[0:2] != "0x":
                return "0x{}".format(ss58_decode(value, ss58_format))

        return value

    def convert_storage_parameter(self, scale_type: str, value: Any):
        return self._convert_storage_parameter(
            scale_type, value, self.runtime_config.ss58_format
        )

    def to_hex(self) -> Optional[str]:
        """
        Returns a Hex-string representation of current StorageKey data

        Returns:
            Hex string
        """
        if self.data:
            return f"0x{self.data.hex()}"
        return None

    def generate(self) -> bytes:
        """
        Generate a storage key for current specified pallet/function/params
        """

        # Search storage call in metadata
        assert self.pallet is not None
        assert self.storage_function is not None
        metadata_pallet = self.metadata.get_metadata_pallet(self.pallet)

        if not metadata_pallet:
            raise StorageFunctionNotFound(f'Pallet "{self.pallet}" not found')

        self.metadata_storage_function = metadata_pallet.get_storage_function(
            self.storage_function
        )

        if not self.metadata_storage_function:
            raise StorageFunctionNotFound(
                f'Storage function "{self.pallet}.{self.storage_function}" not found'
            )

        # Process specific type of storage function
        self.value_scale_type = self.metadata_storage_function.get_value_type_string()
        param_types = self.metadata_storage_function.get_params_type_string()

        hashers = self.metadata_storage_function.get_param_hashers()

        storage_hash = xxh128(
            metadata_pallet.value["storage"]["prefix"].encode()
        ) + xxh128(self.storage_function.encode())

        # Encode parameters
        self.params_encoded = []
        if self.params:
            for idx, param in enumerate(self.params):
                if type(param) is ScaleBytes:
                    # Already encoded
                    self.params_encoded.append(param)
                else:
                    param = self.convert_storage_parameter(param_types[idx], param)
                    param_obj = self.runtime_config.create_scale_object(
                        type_string=param_types[idx]
                    )
                    self.params_encoded.append(param_obj.encode(param))

            for idx, param in enumerate(self.params_encoded):
                # Get hasher associated with param
                try:
                    param_hasher = hashers[idx]
                except IndexError:
                    raise ValueError(f"No hasher found for param #{idx + 1}")

                params_key = bytes()

                # Convert param to bytes
                if type(param) is str:
                    params_key += binascii.unhexlify(param)
                elif type(param) is ScaleBytes:
                    params_key += param.data
                elif isinstance(param, ScaleDecoder):
                    assert param.data is not None
                    params_key += param.data.data

                try:
                    hasher_fn = PARAM_HASHERS[param_hasher or "Twox128"]
                except KeyError:
                    raise ValueError('Unknown storage hasher "{}"'.format(param_hasher))

                storage_hash += hasher_fn(params_key)

        self.data = storage_hash

        return self.data

    def decode_scale_value(self, data: Optional[ScaleBytes] = None) -> ScaleType:
        result_found = False
        assert self.metadata_storage_function is not None
        assert self.value_scale_type is not None

        if data is not None:
            change_scale_type = self.value_scale_type
            result_found = True
        elif self.metadata_storage_function.value["modifier"] == "Default":
            # Fallback to default value of storage function if no result
            change_scale_type = self.value_scale_type
            data = ScaleBytes(
                self.metadata_storage_function.value_object["default"].value_object
            )
        else:
            # No result is interpreted as an Option<...> result
            change_scale_type = f"Option<{self.value_scale_type}>"
            data = ScaleBytes(
                self.metadata_storage_function.value_object["default"].value_object
            )

        # Decode SCALE result data
        updated_obj = self.runtime_config.create_scale_object(
            type_string=change_scale_type, data=data, metadata=self.metadata
        )
        updated_obj.decode()
        updated_obj.meta_info = {"result_found": result_found}

        return updated_obj

    def __repr__(self):
        if self.pallet and self.storage_function:
            return f"<StorageKey(pallet={self.pallet}, storage_function={self.storage_function}, params={self.params})>"
        elif self.data:
            return f"<StorageKey(data=0x{self.data.hex()})>"
        else:
            return repr(self)
