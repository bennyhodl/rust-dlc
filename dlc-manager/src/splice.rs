//! Module for working with DLC inputs

use bitcoin::Transaction;
use dlc::dlc_input::DlcInputInfo;
use dlc_messages::FundingInput;
use secp256k1_zkp::{ecdsa::Signature, PublicKey, Secp256k1, Verification};

// todo: definitely test
/// Get the DlcInputInfo from FundingInputs
pub fn get_dlc_inputs_from_funding_inputs(funding_inputs: &[FundingInput]) -> Vec<DlcInputInfo> {
    funding_inputs
        .iter()
        .filter(|i| i.dlc_input.is_some())
        .collect::<Vec<&FundingInput>>()
        .into_iter()
        .map(|i| i.into())
        .collect::<Vec<DlcInputInfo>>()
}

/// Verify a DLC funding input signature
#[allow(dead_code)]
pub fn verify_dlc_funding_input_signature<V: Verification>(
    secp: &Secp256k1<V>,
    fund_transaction: &Transaction,
    input_index: usize,
    dlc_input: &DlcInputInfo,
    signature: &Signature,
    pubkey: &PublicKey,
) -> Result<(), dlc::Error> {
    let funding_script = dlc::dlc_input::create_dlc_input_funding_script(dlc_input);
    dlc::verify_tx_input_sig(
        secp,
        signature,
        fund_transaction,
        input_index,
        &funding_script,
        dlc_input.fund_amount,
        pubkey,
    )
}
