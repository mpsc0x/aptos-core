/// This defines the fungible asset module that can issue fungible asset of any `Metadata` object. The
/// metadata object can be any object that equipped with `Metadata` resource.
///
/// The dispatchable_fungible_asset wraps the existing fungible_asset module and adds the ability for token issuer
/// to customize the logic for withdraw and deposit operations. For example:
///
/// - Deflation token: a fixed percentage of token will be destructed upon transfer.
/// - Transfer allowlist: token can only be transfered to addresses in the allow list.
/// - Predicated transfer: transfer can only happen when some certain predicate has been met.
/// - Loyalty token: a fixed loyalty will be paid to a designated address when a fungible asset transfer happens
///
/// See AIP-73 for further discussion
///
module aptos_framework::dispatchable_fungible_asset {
    use aptos_framework::fungible_asset::{Self, FungibleAsset, TransferRef};
    use aptos_framework::function_info::FunctionInfo;
    use aptos_framework::object::{Self, ConstructorRef, Object};

    use std::error;
    use std::signer;

    /// TransferRefStore doesn't exist on the fungible asset type.
    const ESTORE_NOT_FOUND: u64 = 18;
    /// Recipient is not getting the guaranteed value;
    const EAMOUNT_MISMATCH: u64 = 4;

    struct TransferRefStore has key {
        transfer_ref: TransferRef
    }

    public fun register_dispatch_functions(
        constructor_ref: &ConstructorRef,
        withdraw_function: FunctionInfo,
		deposit_function: FunctionInfo,
    ) {
        fungible_asset::register_dispatch_functions(constructor_ref, withdraw_function, deposit_function);
        let store_obj = &object::generate_signer(constructor_ref);
        move_to<TransferRefStore>(store_obj, TransferRefStore {
            transfer_ref: fungible_asset::generate_transfer_ref(constructor_ref),
        });
    }

    /// Withdraw `amount` of the fungible asset from `store` by the owner.
    ///
    /// The semantics of deposit will be governed by the function specified in DispatchFunctionStore.
    public fun withdraw<T: key>(
        owner: &signer,
        store: Object<T>,
        amount: u64,
    ): FungibleAsset acquires TransferRefStore {
        if(fungible_asset::is_dispatchable(store)) {
            let func = fungible_asset::withdraw_function(store);
            dispatchable_withdraw(
                signer::address_of(owner),
                store,
                amount,
                borrow_transfer_ref(store),
                &func,
            )
        } else {
            fungible_asset::withdraw(owner, store, amount)
        }
    }

    /// Deposit `amount` of the fungible asset to `store`.
    ///
    /// The semantics of deposit will be governed by the function specified in DispatchFunctionStore.
    public fun deposit<T: key>(
        store: Object<T>,
        fa: FungibleAsset
    ) acquires TransferRefStore {
        if(fungible_asset::is_dispatchable(store)) {
            let func = fungible_asset::deposit_function(store);
            dispatchable_deposit(
                store,
                fa,
                borrow_transfer_ref(store),
                &func
            )
        } else {
            fungible_asset::deposit(store, fa)
        }
    }

    inline fun borrow_transfer_ref<T: key>(
        metadata: Object<T>
    ): &TransferRef acquires TransferRefStore {
        let metadata_addr = object::object_address(&fungible_asset::store_metadata(metadata));
        assert!(exists<TransferRefStore>(metadata_addr), error::not_found(ESTORE_NOT_FOUND));
        &borrow_global<TransferRefStore>(metadata_addr).transfer_ref
    }

    /// A transfer with a fixed amount debited from the sender
    public fun transfer_fixed_send<T: key>(
        _sender: &signer,
        from: Object<T>,
        to: Object<T>,
        send_amount: u64,
    ) acquires TransferRefStore {
        let fa = fungible_asset::withdraw_with_ref(borrow_transfer_ref(from), from, send_amount);
        deposit(to, fa);
    }

    /// A transfer with a fixed amount credited to the recipient
    public fun transfer_fixed_receive<T: key>(
        sender: &signer,
        from: Object<T>,
        to: Object<T>,
        receive_amount: u64,
    ) acquires TransferRefStore {
        let fa = withdraw(sender, from, receive_amount);
        fungible_asset::deposit_with_ref(borrow_transfer_ref(from), to, fa);
    }

    native fun dispatchable_withdraw<T: key>(
        owner: address,
        store: Object<T>,
        amount: u64,
        transfer_ref: &TransferRef,
        function: &FunctionInfo,
    ): FungibleAsset;

    native fun dispatchable_deposit<T: key>(
        store: Object<T>,
        fa: FungibleAsset,
        transfer_ref: &TransferRef,
        function: &FunctionInfo,
    );
}
