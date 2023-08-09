// Copyright © Aptos Foundation

use crate::BlockPartitioner;
use aptos_types::{
    block_executor::partitioner::{
        CrossShardDependencies, SubBlock, SubBlocksForShard, TransactionWithDependencies,
    },
    transaction::analyzed_transaction::AnalyzedTransaction,
};
use aptos_types::block_executor::partitioner::PartitionedTransactions;

/// An implementation of partitioner that splits the transactions into equal-sized chunks.
pub struct UniformPartitioner {}

impl BlockPartitioner for UniformPartitioner {
    fn partition(
        &self,
        transactions: Vec<AnalyzedTransaction>,
        num_shards: usize,
    ) -> PartitionedTransactions {
        let total_txns = transactions.len();
        if total_txns == 0 {
            return PartitionedTransactions::empty();
        }
        let txns_per_shard = (total_txns as f64 / num_shards as f64).ceil() as usize;

        let mut result: Vec<SubBlocksForShard<AnalyzedTransaction>> = Vec::new();
        let mut global_txn_counter: usize = 0;
        for (shard_id, chunk) in transactions.chunks(txns_per_shard).enumerate() {
            let twds: Vec<TransactionWithDependencies<AnalyzedTransaction>> = chunk
                .iter()
                .map(|t| {
                    TransactionWithDependencies::new(t.clone(), CrossShardDependencies::default())
                })
                .collect();
            let sub_block = SubBlock::new(global_txn_counter, twds);
            global_txn_counter += sub_block.num_txns();
            result.push(SubBlocksForShard::new(shard_id, vec![sub_block]));
        }
        PartitionedTransactions::new(result, vec![])
    }
}