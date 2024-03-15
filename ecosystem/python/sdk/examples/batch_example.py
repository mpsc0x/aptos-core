# Copyright © Aptos Foundation
# SPDX-License-Identifier: Apache-2.0

import asyncio
from typing import Any, Dict, List, Optional

from aptos_sdk.account import Account
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.async_client import FaucetClient, RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionArgument,
    TransactionPayload,
)

from .common import FAUCET_URL, NODE_URL


async def batch_transfer(
    rest_client: RestClient,
    sender: Account,
    recipients: List[AccountAddress],
    amounts: List[int],
) -> Dict[str, Any]:
    transaction_arguments = [
        TransactionArgument(
            recipients, Serializer.sequence_serializer(Serializer.struct)
        ),
        TransactionArgument(amounts, Serializer.sequence_serializer(Serializer.u64)),
    ]

    payload = EntryFunction.natural(
        "0x1::aptos_account",
        "batch_transfer",
        [],
        transaction_arguments,
    )

    signed_transaction = await rest_client.create_bcs_signed_transaction(
        sender, TransactionPayload(payload)
    )
    return await rest_client.submit_and_wait_for_bcs_transaction(signed_transaction)


async def maybe_balance(
    rest_client: RestClient, account_address: AccountAddress
) -> Optional[int]:
    try:
        return await rest_client.account_balance(account_address)
    except Exception:
        return None


async def main():
    rest_client = RestClient(NODE_URL)
    faucet_client = FaucetClient(FAUCET_URL, rest_client)  # <:!:section_1

    try:
        alice = Account.load("alice.apt")
    except Exception:
        alice = Account.generate()
        alice.store("alice.apt")
        await faucet_client.fund_account(alice.address(), 100_000_000)

    bob = Account.generate()
    carol = Account.generate()
    david = Account.generate()
    elle = Account.generate()

    print("\n=== Addresses ===")
    print(f"Alice: {alice.address()}")
    print(f"Bob: {bob.address()}")
    print(f"Carol: {carol.address()}")
    print(f"David: {david.address()}")
    print(f"Elle: {elle.address()}")

    print("\n=== Initial Balances ===")
    alice_balance = rest_client.account_balance(alice.address())
    bob_balance = maybe_balance(rest_client, bob.address())
    carol_balance = maybe_balance(rest_client, carol.address())
    david_balance = maybe_balance(rest_client, david.address())
    elle_balance = maybe_balance(rest_client, elle.address())
    [
        alice_balance,
        bob_balance,
        carol_balance,
        david_balance,
        elle_balance,
    ] = await asyncio.gather(
        *[alice_balance, bob_balance, carol_balance, david_balance, elle_balance]
    )
    print(f"Alice: {alice_balance}")
    print(f"Bob: {bob_balance}")
    print(f"Carol: {carol_balance}")
    print(f"David: {david_balance}")
    print(f"Elle: {elle_balance}")

    recipients = [bob.address(), carol.address(), david.address(), elle.address()]
    amounts = [1000, 2000, 3000, 4000]
    txn_result = await batch_transfer(rest_client, alice, recipients, amounts)
    version = txn_result["version"]
    print("\n=== Transaction ===")
    print(f"https://explorer.aptoslabs.com/txn/{version}?network=testnet\n")

    print("\n=== Final Balances ===")
    alice_balance = rest_client.account_balance(alice.address())
    bob_balance = maybe_balance(rest_client, bob.address())
    carol_balance = maybe_balance(rest_client, carol.address())
    david_balance = maybe_balance(rest_client, david.address())
    elle_balance = maybe_balance(rest_client, elle.address())
    [
        alice_balance,
        bob_balance,
        carol_balance,
        david_balance,
        elle_balance,
    ] = await asyncio.gather(
        *[alice_balance, bob_balance, carol_balance, david_balance, elle_balance]
    )
    print(f"Alice: {alice_balance}")
    print(f"Bob: {bob_balance}")
    print(f"Carol: {carol_balance}")
    print(f"David: {david_balance}")
    print(f"Elle: {elle_balance}")

    await rest_client.close()


if __name__ == "__main__":
    asyncio.run(main())
