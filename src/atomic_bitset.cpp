/*
 *  This file is part of Peredvizhnikov Engine
 *  Copyright (C) 2023 Eduard Permyakov 
 *
 *  Peredvizhnikov Engine is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Peredvizhnikov Engine is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

export module atomic_bitset;

import shared_ptr;
import snap_collector;
import assert;
import logger;

import <atomic>;
import <memory>;
import <optional>;
import <set>;
import <bit>;
import <vector>;

namespace pe{

/* An aribitrary-sized bitset supporting atomic operations.
 * Note that since the contents of the bitset may be in
 * disjoint memory words that cannot all be read atomically,
 * special care must be taken to support operations such as
 * FindFirstSet. More precisely, it is necessary to take a 
 * linearizable snapshot of all the disjoint memory words
 * and operate on that. If we are naively reading the words
 * one-by-one, then the following situation is possible:
 *
 *     Let 'bitset' be a 128-bit set stored in 2 memory words.
 *     Initially all bits are set to 0.
 * 
 *     Thread 0:        Thread 1:
 *     --------         --------
 * 1                    first = bitset.FindFirstSet()
 * 2                    read the first memory word and test bits (all clear)
 * 3.  bitset.Set(0)
 * 4.  bitset.Set(64)
 * 5.                   read the second memory word and test bits (found set bit)
 * 6.                   return first = 64
 *
 *     The result is inconsistent because setting bit 64
 *     is sequenced after setting bit 0. Thus, if bit 64 is
 *     set, it must be true that bit 0 is also set.
 */
export
class AtomicBitset
{
private:

    using WordSnapCollector = SnapCollector<std::atomic<uint64_t>, uint64_t>;

    std::size_t                                      m_count;
    std::size_t                                      m_nwords;
    std::unique_ptr<std::atomic<uint64_t>[]>         m_words;
    std::atomic<std::atomic<uint64_t>*>              m_words_ptr;
    mutable pe::atomic_shared_ptr<WordSnapCollector> m_psc;

    std::pair<uint64_t, uint64_t> bit_location(std::size_t pos) const
    {
        uint64_t word = pos / 64;
        uint64_t bit = 63 - (pos % 64);
        return {word, bit};
    }

    pe::shared_ptr<WordSnapCollector> acquire_snap_collector() const
    {
    retry:
        auto sc = m_psc.load(std::memory_order_acquire);
        if(sc->IsActive())
            return sc;

        auto new_sc = pe::make_shared<WordSnapCollector>(true);
        if(m_psc.compare_exchange_strong(sc, new_sc,
            std::memory_order_release, std::memory_order_acquire)) {
            return new_sc;
        }

        if(!sc->IsActive())
            goto retry;

        return sc;
    }

    void collect_snapshot(pe::shared_ptr<WordSnapCollector> sc) const
    {
        int curr_idx = 0;
        auto words = m_words_ptr.load(std::memory_order_acquire);
        while(sc->IsActive()) {
            if(curr_idx == m_nwords) {
                sc->BlockFurtherNodes();
                sc->Deactivate();
                break;
            }
            sc->AddNode(&m_words[curr_idx], 
                words[curr_idx].load(std::memory_order_relaxed));
            curr_idx++;
        }
        sc->BlockFurtherReports();
    }

    std::unique_ptr<uint64_t[]> reconstruct_using_reports(pe::shared_ptr<WordSnapCollector> sc) const
    {
        auto words = sc->ReadPointersWithComparator([](const auto& a, const auto& b){
            return a.m_ptr < b.m_ptr;
        });
        auto reports = sc->ReadReports();
        auto ret = std::make_unique<uint64_t[]>(m_nwords);
        pe::assert(std::size(words) == m_nwords);

        int idx = 0;
        for(const auto& desc : words) {
            ret[idx++] = desc.m_value;
        }

        uint64_t iword, ibit;
        for(const auto& report : reports) {
            switch(report.m_type) {
            case WordSnapCollector::Report::Type::eInsert:
                std::tie(iword, ibit) = bit_location(report.m_value);
                ret[iword] |= (uint64_t(0b1) << ibit);
                break;
            case WordSnapCollector::Report::Type::eDelete:
                std::tie(iword, ibit) = bit_location(report.m_value);
                ret[iword] &= ~(uint64_t(0b1) << ibit);
                break;
            }
        }
        return ret;
    }

public:

    AtomicBitset(std::size_t count)
        : m_count{count}
        , m_nwords{(count / 64) + !!(count % 64)}
        , m_words{new std::atomic<uint64_t>[m_nwords]}
        , m_words_ptr{}
        , m_psc{pe::make_shared<WordSnapCollector>(false)}
    {
        /* Safely initialize and "publish" the buffer
         * of atomics.
         */
        for(int i = 0; i < m_nwords; i++) {
            m_words[i].store(0, std::memory_order_relaxed);
        }
        m_words_ptr.store(m_words.get(), std::memory_order_release);
    }

    ~AtomicBitset()
    {
        m_psc.store(nullptr, std::memory_order_relaxed);
    }

    bool Test(std::size_t pos, std::memory_order order = std::memory_order_seq_cst) const
    {
        if(pos >= m_count) [[unlikely]]
            throw std::out_of_range{"Bit index out of range."};

        auto [iword, ibit] = bit_location(pos);
        auto words = m_words_ptr.load(std::memory_order_acquire);
        uint64_t word = words[iword].load(order);
        bool bitset = !!(word & (uint64_t(0b1) << ibit));

        auto sc = m_psc.load(std::memory_order_acquire);
        if(sc->IsActive()) {
            if(bitset) {
                sc->Report({nullptr, pos, WordSnapCollector::Report::Type::eInsert});
            }else{
                sc->Report({nullptr, pos, WordSnapCollector::Report::Type::eDelete});
            }
        }
        return bitset;
    }

    void Set(std::size_t pos, std::memory_order order = std::memory_order_seq_cst)
    {
        if(pos >= m_count) [[unlikely]]
            throw std::out_of_range{"Bit index out of range."};

        auto [iword, ibit] = bit_location(pos);
        auto words = m_words_ptr.load(std::memory_order_acquire);
        uint64_t desired;
        uint64_t expected = words[iword].load(std::memory_order_relaxed);
        do{
            desired = (expected | (uint64_t(0b1) << ibit));
        }while(!words[iword].compare_exchange_weak(expected, desired,
            order, std::memory_order_relaxed));

        auto sc = m_psc.load(std::memory_order_acquire);
        if(sc->IsActive()) {
            sc->Report({nullptr, pos, WordSnapCollector::Report::Type::eInsert});
        }
    }

    void Clear(std::size_t pos, std::memory_order order = std::memory_order_seq_cst)
    {
        if(pos >= m_count) [[unlikely]]
            throw std::out_of_range{"Bit index out of range."};

        auto [iword, ibit] = bit_location(pos);
        auto words = m_words_ptr.load(std::memory_order_acquire);
        uint64_t desired;
        uint64_t expected = words[iword].load(std::memory_order_relaxed);
        do{
            desired = (expected & ~(uint64_t(0b1) << ibit));
        }while(!words[iword].compare_exchange_weak(expected, desired,
            order, std::memory_order_relaxed));

        auto sc = m_psc.load(std::memory_order_acquire);
        if(sc->IsActive()) {
            sc->Report({nullptr, pos, WordSnapCollector::Report::Type::eDelete});
        }
    }

    std::unique_ptr<uint64_t[]> TakeSnapshot() const
    {
        auto sc = acquire_snap_collector();
        collect_snapshot(sc);
        return reconstruct_using_reports(sc);
    }

    std::optional<std::size_t> FindFirstSet() const
    {
        auto words = m_words_ptr.load(std::memory_order_acquire);
        if(m_nwords > 1) {
            auto snapshot = TakeSnapshot();
            uint64_t checked = 0;
            for(int i = 0; i < m_nwords; i++) {
                uint64_t lz;
                asm volatile(
                    "lzcnt %1, %0\n"
                    : "=r" (lz)
                    : "r" (snapshot[i])
                );
                if(lz < 64)
                    return {checked + lz};
                checked += 64;
            }
            return std::nullopt;
        }else{
            uint64_t word = words[0].load(std::memory_order_relaxed);
            uint64_t lz;
            asm volatile(
                "lzcnt %1, %0\n"
                : "=r" (lz)
                : "r" (word)
            );
            if(lz == 64)
                return std::nullopt;
            return {lz};
        }
    }

    std::size_t CountSetBits() const
    {
        auto words = m_words_ptr.load(std::memory_order_acquire);
        if(m_nwords > 1) {
            auto snapshot = TakeSnapshot();
            uint64_t total = 0;
            for(int i = 0; i < m_nwords; i++) {
                uint64_t cnt;
                asm volatile(
                    "popcnt %1, %0\n"
                    : "=r" (cnt)
                    : "r" (snapshot[i])
                );
                total += cnt;
            }
            return total;
        }else{
            uint64_t word = words[0].load(std::memory_order_relaxed);
            uint64_t cnt;
            asm volatile(
                "popcnt %1, %0\n"
                : "=r" (cnt)
                : "r" (word)
            );
            return cnt;
        }
    }

    std::size_t Size() const
    {
        return m_count;
    }
};

} //namespace pe

