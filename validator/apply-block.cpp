/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2017-2020 Telegram Systems LLP
*/
#include "adnl/utils.hpp"
#include "td/actor/MultiPromise.h"
#include "ton/ton-io.hpp"
#include "validator/fabric.h"
#include "validator/invariants.hpp"

#include "apply-block.hpp"
#include <chrono>
#include <fstream>
#include <atomic>
#include "block/block-auto.h"
#include "block/block-parse.h"


namespace ton {

namespace validator {

void ApplyBlock::abort_query(td::Status reason) {
  if (promise_) {
    VLOG(VALIDATOR_WARNING) << "aborting apply block query for " << id_.to_str() << ": " << reason;
    promise_.set_error(std::move(reason));
  }
  stop();
}

void ApplyBlock::finish_query() {
  VLOG(VALIDATOR_DEBUG) << "successfully finishing apply block query in " << perf_timer_.elapsed() << " s";
  handle_->set_processed();
  ValidatorInvariants::check_post_apply(handle_);

  if (promise_) {
    promise_.set_value(td::Unit());
  }
  stop();
}

void ApplyBlock::alarm() {
  abort_query(td::Status::Error(ErrorCode::timeout, "timeout"));
}

void ApplyBlock::start_up() {
  VLOG(VALIDATOR_DEBUG) << "running apply_block for " << id_.to_str() << ", mc_seqno=" << masterchain_block_id_.seqno();

  if (id_.is_masterchain()) {
    masterchain_block_id_ = id_;
  }

  alarm_timestamp() = timeout_;

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<BlockHandle> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
    } else {
      td::actor::send_closure(SelfId, &ApplyBlock::got_block_handle, R.move_as_ok());
    }
  });

  td::actor::send_closure(manager_, &ValidatorManager::get_block_handle, id_, true, std::move(P));
}

void ApplyBlock::got_block_handle(BlockHandle handle) {
  VLOG(VALIDATOR_DEBUG) << "got_block_handle";
  handle_ = std::move(handle);

  if (handle_->is_applied() && (!handle_->id().is_masterchain() || handle_->processed())) {
    finish_query();
    return;
  }

  if (handle_->is_applied()) {
    VLOG(VALIDATOR_DEBUG) << "already applied";
    auto P =
        td::PromiseCreator::lambda([SelfId = actor_id(this), seqno = handle_->id().id.seqno](td::Result<BlockIdExt> R) {
          R.ensure();
          auto h = R.move_as_ok();
          if (h.id.seqno < seqno) {
            td::actor::send_closure(SelfId, &ApplyBlock::written_block_data);
          } else {
            td::actor::send_closure(SelfId, &ApplyBlock::finish_query);
          }
        });
    td::actor::send_closure(manager_, &ValidatorManager::get_top_masterchain_block, std::move(P));
    return;
  }

  if (handle_->id().id.seqno == 0) {
    VLOG(VALIDATOR_DEBUG) << "seqno == 0";
    written_block_data();
    return;
  }

  if (handle_->is_archived()) {
    VLOG(VALIDATOR_DEBUG) << "already archived";
    finish_query();
    return;
  }

  if (handle_->received()) {
    VLOG(VALIDATOR_DEBUG) << "already received";
    written_block_data();
    return;
  }

  if (block_.not_null()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &ApplyBlock::written_block_data);
      }
    });

    VLOG(VALIDATOR_DEBUG) << "storing block data";
    td::actor::send_closure(manager_, &ValidatorManager::set_block_data, handle_, block_, std::move(P));
  } else {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), handle = handle_](td::Result<td::Ref<BlockData>> R) {
      CHECK(handle->received());
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &ApplyBlock::written_block_data);
      }
    });

    VLOG(VALIDATOR_DEBUG) << "wait for block data";
    td::actor::send_closure(manager_, &ValidatorManager::wait_block_data, handle_, apply_block_priority(), timeout_,
                            std::move(P));
  }
}

void ApplyBlock::written_block_data() {
  VLOG(VALIDATOR_DEBUG) << "written_block_data";
  if (!handle_->id().seqno()) {
    CHECK(handle_->inited_split_after());
    CHECK(handle_->inited_state_root_hash());
    CHECK(handle_->inited_logical_time());
  } else {
    if (handle_->id().is_masterchain() && !handle_->inited_proof()) {
      abort_query(td::Status::Error(ErrorCode::notready, "proof is absent"));
      return;
    }
    if (!handle_->id().is_masterchain() && !handle_->inited_proof_link()) {
      abort_query(td::Status::Error(ErrorCode::notready, "proof link is absent"));
      return;
    }
    CHECK(handle_->inited_merge_before());
    CHECK(handle_->inited_split_after());
    CHECK(handle_->inited_prev());
    CHECK(handle_->inited_state_root_hash());
    CHECK(handle_->inited_logical_time());
  }
  if (handle_->is_applied() && handle_->processed()) {
    finish_query();
  } else {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Ref<ShardState>> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &ApplyBlock::got_cur_state, R.move_as_ok());
      }
    });

    VLOG(VALIDATOR_DEBUG) << "wait_block_state";
    td::actor::send_closure(manager_, &ValidatorManager::wait_block_state, handle_, apply_block_priority(), timeout_,
                            true, std::move(P));
  }
}

void ApplyBlock::got_cur_state(td::Ref<ShardState> state) {
  VLOG(VALIDATOR_DEBUG) << "got_cur_state";
  state_ = std::move(state);
  CHECK(handle_->received_state());
  written_state();
}

void ApplyBlock::written_state() {
  VLOG(VALIDATOR_DEBUG) << "written_state";
  if (handle_->is_applied() && handle_->processed()) {
    finish_query();
    return;
  }
  VLOG(VALIDATOR_DEBUG) << "setting next for parents";

  if (handle_->id().id.seqno != 0 && !handle_->is_applied()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &ApplyBlock::written_next);
      }
    });

    td::MultiPromise mp;
    auto g = mp.init_guard();
    g.add_promise(std::move(P));

    td::actor::send_closure(manager_, &ValidatorManager::set_next_block, handle_->one_prev(true), id_, g.get_promise());
    if (handle_->merge_before()) {
      td::actor::send_closure(manager_, &ValidatorManager::set_next_block, handle_->one_prev(false), id_,
                              g.get_promise());
    }
  } else {
    written_next();
  }
}

void ApplyBlock::written_next() {
  VLOG(VALIDATOR_DEBUG) << "written_next";
  if (handle_->is_applied() && handle_->processed()) {
    finish_query();
    return;
  }

  VLOG(VALIDATOR_DEBUG) << "applying parents";

  if (handle_->id().id.seqno != 0 && !handle_->is_applied()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error_prefix("prev: "));
      } else {
        td::actor::send_closure(SelfId, &ApplyBlock::applied_prev);
      }
    });

    td::MultiPromise mp;
    auto g = mp.init_guard();
    g.add_promise(std::move(P));
    BlockIdExt m = masterchain_block_id_;
    if (id_.is_masterchain()) {
      m = id_;
    }
    run_apply_block_query(handle_->one_prev(true), td::Ref<BlockData>{}, m, manager_, timeout_, g.get_promise());
    if (handle_->merge_before()) {
      run_apply_block_query(handle_->one_prev(false), td::Ref<BlockData>{}, m, manager_, timeout_, g.get_promise());
    }
  } else {
    applied_prev();
  }
}

void ApplyBlock::applied_prev() {
  VLOG(VALIDATOR_DEBUG) << "applying parents";
  VLOG(VALIDATOR_DEBUG) << "applied_prev, waiting manager's confirm";
  if (!id_.is_masterchain()) {
    handle_->set_masterchain_ref_block(masterchain_block_id_.seqno());
  }

  // === OBSERVER: zero-copy, in-memory, no DB ===
  if (block_.not_null()) {
    static std::atomic<int> otx{0};
    static std::atomic<int> omg{0};
    constexpr int OMAX = 500;

    if (otx < OMAX || omg < OMAX) {
      auto wms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      td::uint32 but = handle_->inited_unix_time() ? handle_->unix_time() : 0u;
      long long lat = wms - (long long)but * 1000;
      std::string bid = handle_->id().id.to_str();

      auto root = block_->root_cell();  // already in memory
      block::gen::Block::Record blk;
      block::gen::BlockExtra::Record extra;

      if (tlb::unpack_cell(root, blk) &&
          tlb::unpack_cell(std::move(blk.extra), extra)) {

        vm::AugmentedDictionary ad{
            vm::load_cell_slice_ref(extra.account_blocks), 256,
            block::tlb::aug_ShardAccountBlocks};

        td::Bits256 ca; ca.set_zero();
        bool af = true;
        while (otx < OMAX || omg < OMAX) {
          auto av = ad.extract_value(
              ad.vm::DictionaryFixed::lookup_nearest_key(ca.bits(), 256, true, af));
          if (av.is_null()) break;
          af = false;

          block::gen::AccountBlock::Record ab;
          if (!tlb::csr_unpack(std::move(av), ab)) continue;
          std::string acc = ab.account_addr.to_hex();

          vm::AugmentedDictionary td2{vm::DictNonEmpty(),
              std::move(ab.transactions), 64, block::tlb::aug_AccountTransactions};
          td::BitArray<64> cl; cl.set_zero();
          bool tf = true;

          while (otx < OMAX || omg < OMAX) {
            auto tr = td2.extract_value_ref(
                td2.vm::DictionaryFixed::lookup_nearest_key(cl.bits(), 64, true, tf));
            if (tr.is_null()) break;
            tf = false;

            block::gen::Transaction::Record tx;
            if (!tlb::unpack_cell(tr, tx)) continue;

            int n = otx.fetch_add(1);
            if (n < OMAX) {
              std::ofstream f("/var/ton-work/observer/confirmed_transactions.jsonl", std::ios::app);
              f << lat << "\t" << bid << "\t" << acc
                << "\t" << tx.lt << "\t" << tx.now << "\t" << tx.outmsg_cnt << "\n";
            }

            // in_msg
            auto& ic = tx.r1.in_msg;
            if (omg < OMAX && ic.not_null() && ic->size() >= 1 && ic->prefetch_ulong(1) == 1) {
              auto mr = ic->prefetch_ref();
              if (mr.not_null()) {
                int mn = omg.fetch_add(1);
                if (mn < OMAX) {
                  auto ms = vm::load_cell_slice(mr);
                  int tg = block::gen::t_CommonMsgInfo.get_tag(ms);
                  const char* mt = tg==0?"int":tg==1?"ext_in":tg==2?"ext_out":"unk";
                  std::ofstream f("/var/ton-work/observer/confirmed_messages.jsonl", std::ios::app);
                  f << lat << "\t" << bid << "\t" << mt << "\tin\t" << acc << "\t" << tx.lt << "\n";
                }
              }
            }

            // out_msgs
            auto& oc = tx.r1.out_msgs;
            if (omg < OMAX && tx.outmsg_cnt > 0 && oc.not_null() && oc->size() >= 1 && oc->prefetch_ulong(1) == 1) {
              auto dr = oc->prefetch_ref();
              if (dr.not_null()) {
                vm::Dictionary od{dr, 15};
                od.check_for_each([&](td::Ref<vm::CellSlice> v, td::ConstBitPtr, int) -> bool {
                  if (omg >= OMAX) return false;
                  auto mr = v->prefetch_ref();
                  if (mr.is_null()) return true;
                  int mn = omg.fetch_add(1);
                  if (mn < OMAX) {
                    auto ms = vm::load_cell_slice(mr);
                    int tg = block::gen::t_CommonMsgInfo.get_tag(ms);
                    const char* mt = tg==0?"int":tg==1?"ext_in":tg==2?"ext_out":"unk";
                    std::ofstream f("/var/ton-work/observer/confirmed_messages.jsonl", std::ios::app);
                    f << lat << "\t" << bid << "\t" << mt << "\tout\t" << acc << "\t" << tx.lt << "\n";
                  }
                  return true;
                });
              }
            }
          }
        }
      }
      if (otx >= OMAX && omg >= OMAX) {
        LOG(WARNING) << "[OBSERVER] done: " << otx.load() << " tx, " << omg.load() << " msg";
      }
    }
  }
  // === END OBSERVER ===

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
    } else {
      td::actor::send_closure(SelfId, &ApplyBlock::applied_set);
    }
  });
  td::actor::send_closure(manager_, &ValidatorManager::new_block, handle_, state_, std::move(P));
}

void ApplyBlock::applied_set() {
  VLOG(VALIDATOR_DEBUG) << "applied_set";
  handle_->set_applied();
  if (handle_->id().seqno() > 0) {
    CHECK(handle_->handle_moved_to_archive());
    CHECK(handle_->moved_to_archive());
  }
  if (handle_->need_flush()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &ApplyBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &ApplyBlock::finish_query);
      }
    });
    VLOG(VALIDATOR_DEBUG) << "flush handle";
    handle_->flush(manager_, handle_, std::move(P));
  } else {
    finish_query();
  }
}

}  // namespace validator

}  // namespace ton
