use anyhow::{Context, Result};
use bytes::BytesMut;
use bytes::{BufMut as _, Bytes};
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use alloy::primitives::{
    utils::{format_units, parse_units},
    Address,
};
use alloy::transports::http::reqwest::Url;
use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::U256,
    providers::{Provider, ProviderBuilder, WalletProvider},
    rpc::types::TransactionRequest,
};
use alloy_primitives::address;
use alloy_signer::Signer;
use alloy_signer_local::{coins_bip39::English, MnemonicBuilder};

struct NarwhalClient {
    target: SocketAddr,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
}

impl NarwhalClient {
    fn new(target: SocketAddr, size: usize, rate: u64, nodes: Vec<SocketAddr>) -> Self {
        NarwhalClient { target, size, rate, nodes }
    }


    async fn connect(&self) -> Result<Framed<TcpStream, LengthDelimitedCodec>> {
        let stream = TcpStream::connect(self.target).await?;
        Ok(Framed::new(stream, LengthDelimitedCodec::new()))
    }

    async fn check_health(&self) -> Result<bool> {
        let mut connection = self.connect().await?;
        let health_check = BytesMut::new().freeze();
        connection.send(health_check).await?;
        match connection.next().await {
            Some(Ok(_)) => Ok(true),
            _ => Ok(false)
        }
    }

    pub async fn submit_batch(&self, batch: Bytes) -> Result<()> {
        let mut connection = self.connect().await?;
        connection.send(batch).await?;
        Ok(())
    }

    async fn listen_for_consensus_blocks(&self) -> Result<()> {
        let mut connection = self.connect().await?;
        while let Some(block) = connection.next().await {
            match block {
                Ok(consensus_block) => {
                    // Handle consensus block
                    info!("Received consensus block: {:?}", consensus_block);
                }
                Err(e) => {
                    warn!("Error receiving consensus block: {}", e);
                }
            }
        }
        Ok(())
    }
}

