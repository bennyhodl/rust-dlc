extern crate bitcoin_rpc_provider;
extern crate bitcoin_test_utils;
extern crate bitcoincore_rpc;
extern crate bitcoincore_rpc_json;
extern crate dlc_manager;

#[macro_use]
#[allow(dead_code)]
mod test_utils;

use bitcoin::Amount;
use bitcoin_test_utils::rpc_helpers::init_clients;
use bitcoincore_rpc::{Client, RpcApi};
use dlc_manager::contract::Contract;
use dlc_manager::manager::Manager;
use dlc_manager::{Blockchain, CachedContractSignerProvider, Oracle, SimpleSigner, Wallet};
use dlc_manager::{ContractId, Storage};
use dlc_messages::Message;
use electrs_blockchain_provider::ElectrsBlockchainProvider;
use lightning::util::ser::Writeable;
use mocks::memory_storage_provider::MemoryStorage;
use mocks::mock_oracle_provider::MockOracle;
use mocks::mock_time::MockTime;
use secp256k1_zkp::PublicKey;
use simple_wallet::SimpleWallet;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::channel,
    Arc, Mutex,
};
use std::thread;
use test_utils::*;

type TestManager = Arc<
    Mutex<
        Manager<
            Arc<SimpleWallet<Arc<ElectrsBlockchainProvider>, Arc<MemoryStorage>>>,
            Arc<
                CachedContractSignerProvider<
                    Arc<SimpleWallet<Arc<ElectrsBlockchainProvider>, Arc<MemoryStorage>>>,
                    SimpleSigner,
                >,
            >,
            Arc<ElectrsBlockchainProvider>,
            Arc<MemoryStorage>,
            Arc<MockOracle>,
            Arc<MockTime>,
            Arc<ElectrsBlockchainProvider>,
            SimpleSigner,
        >,
    >,
>;

#[derive(Eq, PartialEq, Clone)]
enum SpliceTestPath {
    SpliceIn,
    SpliceOut,
}

pub struct SplicedInput {
    pub offer_public_key: PublicKey,
    pub accept_public_key: PublicKey,
    pub alice_public_key: PublicKey,
    pub alice_store: Arc<MemoryStorage>,
    pub bob_public_key: PublicKey,
    pub bob_store: Arc<MemoryStorage>,
    pub contract_id: ContractId,
    pub alice_manager: TestManager,
    pub bob_manager: TestManager,
    pub oracles: MockOracle,
    pub electrs: Arc<ElectrsBlockchainProvider>,
    pub sink_rpc: Client,
}

fn generate_blocks(nb_blocks: u64, electrs: &ElectrsBlockchainProvider, sink_rpc: &Client) {
    let prev_blockchain_height = electrs.get_blockchain_height().unwrap();

    let sink_address = sink_rpc
        .get_new_address(None, None)
        .expect("RPC Error")
        .assume_checked();
    sink_rpc
        .generate_to_address(nb_blocks, &sink_address)
        .expect("RPC Error");

    // Wait for electrs to have processed the new blocks
    let mut cur_blockchain_height = prev_blockchain_height;
    while cur_blockchain_height < prev_blockchain_height + nb_blocks {
        std::thread::sleep(std::time::Duration::from_millis(200));
        cur_blockchain_height = electrs.get_blockchain_height().unwrap();
    }
}

fn create_spliced_input() -> SplicedInput {
    env_logger::try_init().ok();
    let (_, _, sink_rpc) = init_clients();

    let mut alice_oracles = HashMap::with_capacity(1);
    let mut bob_oracles = HashMap::with_capacity(1);
    let test_params = get_single_funded_test_params(1, 1, None);
    for oracle in test_params.oracles.clone() {
        let oracle = Arc::new(oracle);
        alice_oracles.insert(oracle.get_public_key(), Arc::clone(&oracle));
        bob_oracles.insert(oracle.get_public_key(), Arc::clone(&oracle));
    }

    let alice_store = Arc::new(mocks::memory_storage_provider::MemoryStorage::new());
    let bob_store = Arc::new(mocks::memory_storage_provider::MemoryStorage::new());
    let mock_time = Arc::new(mocks::mock_time::MockTime {});
    mocks::mock_time::set_time((EVENT_MATURITY as u64) - 1);

    let electrs = Arc::new(ElectrsBlockchainProvider::new(
        "http://localhost:3004/".to_string(),
        bitcoin::Network::Regtest,
    ));

    let alice_wallet = Arc::new(SimpleWallet::new(
        electrs.clone(),
        alice_store.clone(),
        bitcoin::Network::Regtest,
    ));

    let bob_wallet = Arc::new(SimpleWallet::new(
        electrs.clone(),
        bob_store.clone(),
        bitcoin::Network::Regtest,
    ));

    let alice_fund_address = alice_wallet.get_new_address().unwrap();
    let bob_fund_address = bob_wallet.get_new_address().unwrap();

    sink_rpc
        .send_to_address(
            &alice_fund_address,
            Amount::from_btc(2.0).unwrap(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

    sink_rpc
        .send_to_address(
            &bob_fund_address,
            Amount::from_btc(2.0).unwrap(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

    generate_blocks(6, &electrs, &sink_rpc);

    refresh_wallet(&alice_wallet, Amount::from_sat(200000000));
    refresh_wallet(&bob_wallet, Amount::ZERO);

    let alice_manager = Arc::new(Mutex::new(
        Manager::new(
            Arc::clone(&alice_wallet),
            Arc::clone(&alice_wallet),
            Arc::clone(&electrs),
            alice_store.clone(),
            alice_oracles,
            Arc::clone(&mock_time),
            Arc::clone(&electrs),
        )
        .unwrap(),
    ));

    let bob_manager = Arc::new(Mutex::new(
        Manager::new(
            Arc::clone(&bob_wallet),
            Arc::clone(&bob_wallet),
            Arc::clone(&electrs),
            bob_store.clone(),
            bob_oracles,
            Arc::clone(&mock_time),
            Arc::clone(&electrs),
        )
        .unwrap(),
    ));
    // Use consistent public keys like in manager execution tests
    let alice_pubkey: PublicKey =
        "0218845781f631c48f1c9709e23092067d06837f30aa0cd0544ac887fe91ddd166"
            .parse()
            .unwrap();
    let bob_pubkey: PublicKey =
        "0218845781f631c48f1c9709e23092067d06837f30aa0cd0544ac887fe91ddd166"
            .parse()
            .unwrap();

    let alice_offer = alice_manager
        .lock()
        .unwrap()
        .send_offer(&test_params.contract_input, bob_pubkey)
        .unwrap();

    let alice_offer_msg = Message::Offer(alice_offer.clone());

    let _bob_recv_offer = bob_manager
        .lock()
        .unwrap()
        .on_dlc_message(&alice_offer_msg, alice_pubkey)
        .expect("error receiving offer");

    let (contract_id, alice_pubkey_from_accept, bob_accept_msg) = bob_manager
        .lock()
        .unwrap()
        .accept_contract_offer(&alice_offer.temporary_contract_id)
        .unwrap();

    let bob_accept_msg = Message::Accept(bob_accept_msg);

    let alice_recv_accept = alice_manager
        .lock()
        .unwrap()
        .on_dlc_message(&bob_accept_msg, bob_pubkey)
        .expect("error receiving accept")
        .expect("to create a sign");

    let _dlc_managerbob_recv_sign = bob_manager
        .lock()
        .unwrap()
        .on_dlc_message(&alice_recv_accept, alice_pubkey_from_accept)
        .expect("to receive sign message");

    generate_blocks(10, &electrs, &sink_rpc);

    alice_manager
        .lock()
        .unwrap()
        .periodic_check(false)
        .expect("alice to update the contract");
    bob_manager
        .lock()
        .unwrap()
        .periodic_check(false)
        .expect("bob to update the contract");

    match alice_store
        .get_contract(&contract_id.clone())
        .unwrap()
        .expect("contract to exist")
    {
        Contract::Confirmed(_) => (),
        _ => panic!("contract to be confirmed"),
    };

    let _ = alice_wallet.refresh();
    let _ = bob_wallet.refresh();

    SplicedInput {
        offer_public_key: alice_pubkey,
        accept_public_key: bob_pubkey,
        alice_public_key: alice_pubkey,
        bob_public_key: bob_pubkey,
        alice_store: Arc::clone(&alice_store),
        bob_store: Arc::clone(&bob_store),
        contract_id,
        alice_manager: alice_manager,
        bob_manager: bob_manager,
        oracles: test_params.oracles[0].clone(),
        electrs: Arc::clone(&electrs),
        sink_rpc,
    }
}

fn splice_execution_test(spliced_input: SplicedInput, test_path: SpliceTestPath) {
    let (alice_send, bob_receive) = channel::<Option<Message>>();
    let (bob_send, alice_receive) = channel::<Option<Message>>();
    let (sync_send, sync_receive) = channel::<()>();
    let alice_sync_send = sync_send.clone();
    let bob_sync_send = sync_send;

    let alice_expect_error = Arc::new(AtomicBool::new(false));
    let bob_expect_error = Arc::new(AtomicBool::new(false));

    let alice_expect_error_loop = alice_expect_error.clone();
    let bob_expect_error_loop = bob_expect_error.clone();

    let alice_manager_loop = Arc::clone(&spliced_input.alice_manager);
    let alice_manager_send = Arc::clone(&spliced_input.alice_manager);
    let bob_manager_loop = Arc::clone(&spliced_input.bob_manager);
    let bob_manager_send = Arc::clone(&spliced_input.bob_manager);

    let alice_send_loop = alice_send.clone();
    let bob_send_loop = bob_send.clone();

    let msg_callback = |_msg: &Message| {};

    let alice_handle = receive_loop!(
        alice_receive,
        alice_manager_loop,
        alice_send_loop,
        alice_expect_error_loop,
        alice_sync_send,
        Some,
        msg_callback
    );

    let bob_handle = receive_loop!(
        bob_receive,
        bob_manager_loop,
        bob_send_loop,
        bob_expect_error_loop,
        bob_sync_send,
        Some,
        msg_callback
    );

    let test_params = match test_path {
        SpliceTestPath::SpliceIn => get_splice_in_test_params(vec![spliced_input.oracles.clone()]),
        SpliceTestPath::SpliceOut => {
            get_splice_out_test_params(vec![spliced_input.oracles.clone()])
        }
    };

    let alice_splice_offer = alice_manager_send
        .lock()
        .unwrap()
        .send_splice_offer(
            &test_params.contract_input,
            spliced_input.bob_public_key,
            &spliced_input.contract_id,
        )
        .expect("error sending splice offer");

    let temporary_contract_id = alice_splice_offer.temporary_contract_id;
    alice_send
        .send(Some(Message::Offer(alice_splice_offer.clone())))
        .unwrap();

    assert_contract_state!(alice_manager_send, temporary_contract_id, Offered);

    sync_receive.recv().expect("Error synchronizing");

    assert_contract_state!(bob_manager_send, temporary_contract_id, Offered);

    let (contract_id, _, accept_msg) = bob_manager_send
        .lock()
        .unwrap()
        .accept_contract_offer(&temporary_contract_id)
        .expect("error accepting splice offer");

    assert_contract_state!(bob_manager_send, contract_id, Accepted);

    bob_send.send(Some(Message::Accept(accept_msg))).unwrap();

    sync_receive.recv().expect("Error synchronizing");

    assert_contract_state!(alice_manager_send, contract_id, Signed);

    sync_receive.recv().expect("Error synchronizing");

    assert_contract_state!(bob_manager_send, contract_id, Signed);

    generate_blocks(10, &spliced_input.electrs, &spliced_input.sink_rpc);

    alice_manager_send
        .lock()
        .unwrap()
        .periodic_check(false)
        .expect("alice to update the contract");
    bob_manager_send
        .lock()
        .unwrap()
        .periodic_check(false)
        .expect("bob to update the contract");

    assert_contract_state!(alice_manager_send, contract_id, Confirmed);
    assert_contract_state!(bob_manager_send, contract_id, Confirmed);

    let contract = match spliced_input
        .alice_store
        .get_contract(&contract_id)
        .unwrap()
        .expect("contract to exist")
    {
        Contract::Confirmed(c) => c,
        _ => panic!("contract should be in a confirmed state"),
    };

    let fund_tx = contract.accepted_contract.dlc_transactions.fund.clone();

    let _tx = spliced_input
        .electrs
        .get_transaction(&fund_tx.compute_txid())
        .expect("to have fund tx");

    alice_send.send(None).unwrap();
    bob_send.send(None).unwrap();

    alice_handle.join().unwrap();
    bob_handle.join().unwrap();
}

#[test]
#[ignore]
fn splice_in_test() {
    let spliced_input = create_spliced_input();
    splice_execution_test(spliced_input, SpliceTestPath::SpliceIn);
}

#[test]
#[ignore]
fn splice_out_test() {
    let spliced_input = create_spliced_input();
    splice_execution_test(spliced_input, SpliceTestPath::SpliceOut);
}
