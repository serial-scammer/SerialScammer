from cmath import polar

import pandas as pd
import os

from web3 import Web3

from data_collection.AccountCollector import CreatorCollector, TransactionCollector
from data_collection.EventCollector import ContractEventCollector
from entity.LightNode import LightNode
from entity.blockchain.Address import Pool, Token
from entity.blockchain.Event import TransferEvent, SwapEvent, BurnEvent, MintEvent
from utils.Settings import Setting
from utils.ProjectPath import ProjectPath

path = ProjectPath()
setting = Setting()

bridge_files = {
    "univ2": ["bridge.csv", "bridge_addresses.csv"],
    "panv2": ["bridge.csv"],
}
defi_files = {"univ2": [
    "dex.csv",
    "cex_address.csv",
    "exchange_addresses.csv",
    "factory_addresses.csv",
    "deployer_addresses.csv",
    "proxy_addresses.csv",
    "router_addresses.csv",
],
    "panv2": ["router.csv", "proxy.csv", "deployer.csv", "exchange.csv", "factory.csv"]}
cex_files = {"univ2": ["cex_address.csv", "deposit_addresses.csv", "binance_addresses.csv"],
             "panv2": ["exchange.csv"]}
mev_bot_files = {"univ2": ["mev_bot_addresses.csv", "MEV_bots.csv"], "panv2": ["maestro.csv", "mev_bot.csv"]}
mixer_files = {"univ2": ["tonador_cash.csv"], "panv2": []}
wallet_files = {"univ2": ["wallet_addresses.csv"], "panv2": ["wallet.csv"]}
other_files = {"univ2": ["multisender_addresses.csv", "multisig_addresses.csv"], "panv2": ["multisender.csv", "multisig.csv"]}


def load_full_end_nodes(dex="univ2"):
    (bridge_addresses,
     defi_addresses,
     cex_addresses,
     MEV_addresses,
     mixer_addresses,
     wallet_addresses,
     other_addresses,) = load_end_nodes(dex)
    return bridge_addresses | defi_addresses | cex_addresses | MEV_addresses | mixer_addresses | wallet_addresses | other_addresses


def load_end_nodes(dex="univ2"):
    print("LOAD END NODES")
    bridge_addresses = set()
    defi_addresses = set()
    cex_addresses = set()
    MEV_addresses = set()
    mixer_addresses = set()
    wallet_addresses = set()
    other_addresses = set()
    for bf in bridge_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), bf)
        if os.path.exists(file):
            df = pd.read_csv(file)
            bridge_addresses.update(df["address"].str.lower().values)
    for defi in defi_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), defi)
        if os.path.exists(file):
            df = pd.read_csv(file)
            defi_addresses.update(df["address"].str.lower().values)
    for cex in cex_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), cex)
        if os.path.exists(file):
            df = pd.read_csv(file)
            cex_addresses.update(df["address"].str.lower().values)
    for mev in mev_bot_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), mev)
        if os.path.exists(file):
            df = pd.read_csv(file)
            MEV_addresses.update(df["address"].str.lower().values)
    for mixer in mixer_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), mixer)
        if os.path.exists(file):
            df = pd.read_csv(file)
            mixer_addresses.update(df["address"].str.lower().values)
    for wallet in wallet_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), wallet)
        if os.path.exists(file):
            df = pd.read_csv(file)
            wallet_addresses.update(df["address"].str.lower().values)
    for other in other_files[dex]:
        file = os.path.join(eval("path.{}_public_addresses_path".format(dex)), other)
        if os.path.exists(file):
            df = pd.read_csv(file)
            other_addresses.update(df["address"].str.lower().values)
    return (
        bridge_addresses,
        defi_addresses,
        cex_addresses,
        MEV_addresses,
        mixer_addresses,
        wallet_addresses,
        other_addresses,
    )


def load_creation_info(dex="univ2"):
    print("LOAD CREATION INFO")
    creation_info = dict()
    pool_creation_path = os.path.join(
        eval("path.{}_processed_path".format(dex)), "pool_creation_info.csv"
    )
    token_creation_path = os.path.join(
        eval("path.{}_processed_path".format(dex)), "token_creation_info.csv"
    )
    pool_creations = pd.read_csv(pool_creation_path)
    creation_info.update(
        dict(
            zip(
                pool_creations["contractAddress"].str.lower(),
                pool_creations["contractCreator"].str.lower(),
            )
        )
    )
    token_creations = pd.read_csv(token_creation_path)
    creation_info.update(
        dict(
            zip(
                token_creations["contractAddress"].str.lower(),
                token_creations["contractCreator"].str.lower(),
            )
        )
    )
    return creation_info


def load_pool_info(dex="univ2"):
    pool_infos = pd.read_csv(
        os.path.join(eval("path.{}_processed_path".format(dex)), "pool_info.csv"),
        low_memory=False,
    )
    pool_infos.drop_duplicates(inplace=True)
    tokens = pool_infos[["token0", "token1"]]
    tokens["token0"] = tokens["token0"].str.lower()
    tokens["token1"] = tokens["token1"].str.lower()
    return dict(zip(pool_infos["pool"].str.lower(), tokens.to_dict("records")))


def load_token_info(dex="univ2"):
    token_infos = pd.read_csv(
        os.path.join(eval("path.{}_processed_path".format(dex)), "token_info.csv"),
        low_memory=False,
    )
    token_infos.drop_duplicates(inplace=True)
    infos = token_infos[["name", "symbol", "decimals", "totalSupply"]]
    return dict(zip(token_infos["token"].str.lower(), infos.to_dict("records")))


def load_group_scammers(dex="univ2"):
    group_scammers, scammer_group = dict(), dict()
    file_path = os.path.join(eval('path.{}_processed_path'.format(dex)), "non_swap_simple_rp_scammer_group.csv")
    scammers = set()
    if os.path.exists(file_path):
        groups = pd.read_csv(file_path)
        # group_scammers = groups.groupby("group_id")["scammer"].apply(list).to_dict()
        for idx, row in groups.iterrows():
            group_id = row["group_id"]
            scammer = row["scammer"]
            if group_id not in group_scammers:
                group_scammers[group_id] = list()
            if scammer not in scammers:
                group_scammers[group_id].append(scammer)
                scammer_group[scammer] = group_id
                scammers.add(scammer)
    return group_scammers, scammer_group


def link_pool_and_group(scammer_pools, group_scammers):
    pool_group = dict()
    for group_id in group_scammers.keys():
        scammers = group_scammers[group_id]
        pools = set()
        for s in scammers:
            pools.update(scammer_pools[s])
        for p in pools:
            pool_group[p] = group_id
    return pool_group


def load_rug_pull_dataset(dex="univ2", scammer_file_name="1_pair_scammers.csv", pool_file_name="1_pair_pool_labels.csv"):
    print("LOAD RUG PULL INFO")
    scam_pools = list()
    # scammers = list()
    scammers = pd.read_csv(
        os.path.join(eval("path.{}_processed_path".format(dex)), scammer_file_name)
    )
    index_issue = scammers[(scammers["pool"] == scammers["scammer"])].index
    scammers.drop(index_issue, inplace=True)
    scammers["pool"] = scammers["pool"].str.lower()
    scammers["scammer"] = scammers["scammer"].str.lower()
    # scam_pools.extend(scammers["pool"].unique())
    pool_scammers = scammers.groupby("pool")["scammer"].apply(list).to_dict()
    scammer_pools = scammers.groupby("scammer")["pool"].apply(list).to_dict()
    rp_pools = pd.read_csv(
        os.path.join(
            eval("path.{}_processed_path".format(dex)), pool_file_name
        )
    )
    rp_pools.fillna("", inplace=True)
    rp_pools = rp_pools[rp_pools["is_rp"] != 0]
    rp_pools["pool"] = rp_pools["pool"].str.lower()
    rp_pools["scam_token"] = rp_pools["scam_token"].str.lower()
    scam_pools.extend(rp_pools["pool"].unique())
    scam_token_pool = dict(zip(rp_pools["scam_token"], rp_pools["pool"]))
    return pool_scammers, scam_token_pool, scam_pools, set(scammers["scammer"].str.lower().to_list()), scammer_pools


def load_cluster(name, dex="univ2"):
    c_path = os.path.join(eval(f"path.{dex}_cluster_path"), f"{name}.csv")
    cluster_df = pd.read_csv(c_path)
    clusters = []
    for idx, row in cluster_df.iterrows():
        cluster_node = LightNode.from_dict(row.to_dict())
        clusters.append(cluster_node)
    return clusters


def load_transaction_by_address(address, dex="univ2"):
    transaction_collector = TransactionCollector()
    normal_txs, internal_txs = transaction_collector.get_transactions(address, dex)
    return normal_txs, internal_txs


contract_event_collector = ContractEventCollector()
creator_collector = CreatorCollector()

def load_light_pool(scammer_address, dataloader, dex="univ2"):
    pool_addresses = dataloader.scammer_pools[scammer_address.lower()]
    pool_event_path = eval("path.{}_pool_events_path".format(dex))
    pools = []
    for pool_address in pool_addresses:
        pool_address = Web3.to_checksum_address(pool_address)
        burns_list = contract_event_collector.get_event(
            pool_address, "Burn", pool_event_path, dex
        )
        burns = [BurnEvent().from_dict(e) for e in burns_list]
        mint_list = contract_event_collector.get_event(
            pool_address, "Mint", pool_event_path, dex
        )
        mints = [MintEvent().from_dict(e) for e in mint_list]
        pool_info = dataloader.pool_infos[pool_address.lower()]
        scammers = dataloader.pool_scammers[pool_address.lower()]
        pool_creation = creator_collector.get_pool_creator(pool_address, dex)
        token0 = pool_info["token0"]
        token1 = pool_info["token1"]
        pool = Pool(
            pool_address,
            token0,
            token1,
            scammers,
            mints,
            burns,
            None,
            None,
            pool_creation["contractCreator"],
            pool_creation["txHash"],
        )
        pools.append(pool)
    return pools
def load_pool(scammer_address, dataloader, dex="univ2"):
    pool_addresses = dataloader.scammer_pools[scammer_address.lower()]
    pool_event_path = eval("path.{}_pool_events_path".format(dex))
    contract_event_collector = ContractEventCollector()
    creator_collector = CreatorCollector()
    pools = []
    for pool_address in pool_addresses:
        transfer_list = contract_event_collector.get_event(
            pool_address, "Transfer", pool_event_path, dex
        )
        transfers = [TransferEvent().from_dict(e) for e in transfer_list]
        swaps_list = contract_event_collector.get_event(
            pool_address, "Swap", pool_event_path, dex
        )
        swaps = [SwapEvent().from_dict(e) for e in swaps_list]
        burns_list = contract_event_collector.get_event(
            pool_address, "Burn", pool_event_path, dex
        )
        burns = [BurnEvent().from_dict(e) for e in burns_list]
        mint_list = contract_event_collector.get_event(
            pool_address, "Mint", pool_event_path, dex
        )
        mints = [MintEvent().from_dict(e) for e in mint_list]
        pool_info = dataloader.pool_infos[pool_address.lower()]
        scammers = dataloader.pool_scammers[pool_address.lower()]
        pool_creation = creator_collector.get_pool_creator(pool_address, dex)
        token0 = pool_info["token0"]
        # token0.from_dict(dataloader.token_infos[pool_info["token0"]])
        # token0_creation = creator_collector.get_token_creator(token0.address, dex)
        # token0.creator = token0_creation["contractCreator"]
        # token0.creation_tx = token0_creation["txHash"]
        token1 = pool_info["token1"]
        # token1.from_dict(dataloader.token_infos[pool_info["token1"]])
        # token1_creation = creator_collector.get_token_creator(token1.address, dex)
        # token1.creator = token1_creation["contractCreator"]
        # token1.creation_tx = token1_creation["txHash"]
        pool = Pool(
            pool_address,
            token0,
            token1,
            scammers,
            mints,
            burns,
            swaps,
            transfers,
            pool_creation["contractCreator"],
            pool_creation["txHash"],
        )
        pools.append(pool)
    return pools


class DataLoader(object):
    def __init__(self, dex="univ2"):
        ### ALL ADDRESSES MUST BE IN LOWER CASES ###
        # sets of address
        (
            self.bridge_addresses,
            self.defi_addresses,
            self.cex_addresses,
            self.MEV_addresses,
            self.mixer_addresses,
            self.wallet_addresses,
            self.other_addresses,
        ) = load_end_nodes(dex=dex)
        # key is token/pool address - value is creator address
        self.creators = load_creation_info(dex=dex)
        # contract infos
        self.pool_infos = load_pool_info(dex=dex)
        self.token_infos = load_token_info(dex=dex)
        # rug pull related infos
        (
            self.pool_scammers,
            self.scam_token_pool,
            self.scam_pools,
            self.scammers,
            self.scammer_pools,
        ) = load_rug_pull_dataset(dex=dex, scammer_file_name="filtered_simple_rp_scammers.csv", pool_file_name="filtered_simple_rp_pool.csv")
        self.scammers_set = set(self.scammers)
        self.group_scammers, self.scammer_group = load_group_scammers(dex)
        self.pool_group = link_pool_and_group(self.scammer_pools, self.group_scammers)


if __name__ == "__main__":
    dataloader = DataLoader(dex='panv2')
    # print(load_cluster("cluster_0x7f0a9d794bba0a588f4c8351d8549bb5f76a34c4", dex='univ2'))
    # print(load_token_info(dex='univ2'))
    # pool = load_pool("0x19b98792e98c54f58c705cddf74316aec0999aa6", DataLoader(dex='univ2'))
    # pos = pool.get_high_value_position()
    # print(pool.address)
    # print(pos)
    # print(pool.get_max_swap_value(pos))
    # print(pool.get_total_mint_value(pos))
    # print(pool.get_total_burn_value(pos))
    # print(len(load_full_end_nodes('univ2')))
    # print(len(load_full_end_nodes('panv2')))
    pools = load_light_pool("0xad08da4f51a764c66e6f4b2bfd3bfd04bf73653e", dataloader, dex='panv2')
    for pool in pools:
        print(f"pool address {pool.address} - mints: {len(pool.mints)} burn:{len(pool.burns)}")
        print("Mint.amount0", pool.mints[0].amount0, ".amount1", pool.mints[0].amount1)
        print("Mint.amount0", pool.burns[0].amount0, ".amount1", pool.burns[0].amount1)