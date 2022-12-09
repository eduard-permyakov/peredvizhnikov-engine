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
        while(sc->IsActive()) {
            if(curr_idx == m_nwords) {
                sc->BlockFurtherNodes();
                sc->Deactivate();
                break;
            }
            sc->AddNode(&m_words[curr_idx], 
                m_words[curr_idx].load(std::memory_order_relaxed));
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
        , m_psc{pe::make_shared<WordSnapCollector>(false)}
    {}

    ~AtomicBitset()
    {
        m_psc.store(nullptr, std::memory_order_relaxed);
    }

    bool Test(std::size_t pos, std::memory_order order = std::memory_order_seq_cst) const
    {
        if(pos >= m_count) [[unlikely]]
            throw std::out_of_range{"Bit index out of range."};

        auto [iword, ibit] = bit_location(pos);
        uint64_t word = m_words[iword].load(order);
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
        uint64_t desired;
        uint64_t expected = m_words[iword].load(std::memory_order_relaxed);
        do{
            desired = (expected | (uint64_t(0b1) << ibit));
        }while(!m_words[iword].compare_exchange_weak(expected, desired,
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
        uint64_t desired;
        uint64_t expected = m_words[iword].load(std::memory_order_relaxed);
        do{
            desired = (expected & ~(uint64_t(0b1) << ibit));
        }while(!m_words[iword].compare_exchange_weak(expected, desired,
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
        if(m_nwords > 1) {
            auto snapshot = TakeSnapshot();
            uint64_t checked = 0;
            for(int i = 0; i < m_nwords; i++) {
                uint64_t lz;
                asm volatile(
                    "lzcnt %0, %1\n"
                    : "=r" (lz)
                    : "r" (snapshot[i])
                );
                if(lz < 64)
                    return {checked + lz};
                checked += 64;
            }
            return std::nullopt;
        }else{
            uint64_t word = m_words[0].load(std::memory_order_relaxed);
            uint64_t lz;
            asm volatile(
                "lzcnt %0, %1\n"
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
        if(m_nwords > 1) {
            auto snapshot = TakeSnapshot();
            uint64_t total = 0;
            for(int i = 0; i < m_nwords; i++) {
                uint64_t cnt;
                asm volatile(
                    "popcnt %0, %1\n"
                    : "=r" (cnt)
                    : "r" (snapshot[i])
                );
                total += cnt;
            }
            return total;
        }else{
            uint64_t word = m_words[0].load(std::memory_order_relaxed);
            uint64_t cnt;
            asm volatile(
                "popcnt %0, %1\n"
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

