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

export module hazard_ptr;

import platform;
import logger;
import tls;
import assert;
import concurrency;

import <atomic>;
import <vector>;

namespace pe{

/*****************************************************************************/
/* HAZARD POINTER                                                            */
/*****************************************************************************/

/* Implementation based on the paper "Hazard Pointers: Safe Memory 
 * Reclamation for Lock-Free Objects" by Maged M. Michael.
 */

export
template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
struct HPContext
{
private:

    struct HPRecord
    {
        std::atomic<NodeType*> m_hp[K]{};
        std::atomic<HPRecord*> m_next{};
        std::atomic<bool>      m_active{};

        /* Only touched by the owning thread */
        std::vector<NodeType*> m_rlist{};
        std::size_t            m_rcount{};

        static_assert(std::remove_reference_t<decltype(m_hp[0])>::is_always_lock_free);
        static_assert(decltype(m_next)::is_always_lock_free);
        static_assert(decltype(m_active)::is_always_lock_free);
    };

    class HazardPtr
    {
    private:

        friend struct HPContext;

        NodeType  *m_raw; 
        int        m_idx;
        HPContext& m_ctx;

        HazardPtr(NodeType *raw, int index, HPContext& ctx)
            : m_raw{raw}
            , m_idx{index}
            , m_ctx{ctx}
        {}

    public:

        HazardPtr(HPContext& ctx)
            : m_raw{}
            , m_idx{}
            , m_ctx{ctx}
        {}

        HazardPtr(HazardPtr const&) = delete;
        HazardPtr& operator=(HazardPtr const&) = delete;

        HazardPtr(HazardPtr&& other)
            : m_raw{other.m_raw}
            , m_idx{other.m_idx}
            , m_ctx{other.m_ctx}
        {
            other.m_raw = nullptr;
        }

        HazardPtr& operator=(HazardPtr&& other)
        {
            m_raw = other.m_raw;
            m_idx = other.m_idx;
            other.m_raw = nullptr;
            return *this;
        }

        ~HazardPtr()
        {
            if(m_raw) {
                m_ctx.ReleaseHazard(m_idx);
            }
        }

        NodeType* operator->() const noexcept
        {
            return m_raw;
        }

        NodeType* operator*() const noexcept
        {
            return m_raw;
        }

        explicit operator bool() const noexcept
        {
            return m_raw;
        }
    };

    struct HPRecordGuard
    {
    private:

        HPContext&               m_ctx;
        HPRecord                *m_record;
        std::atomic_flag         m_delete;

    public:

        HPRecordGuard(HPRecordGuard&&) = delete;
        HPRecordGuard(HPRecordGuard const&) = delete;
        HPRecordGuard& operator=(HPRecordGuard&&) = delete;
        HPRecordGuard& operator=(HPRecordGuard const&) = delete;

        HPRecordGuard(HPContext& ctx)
            : m_ctx{ctx}
            , m_record{ctx.AllocateHPRecord()}
            , m_delete{false}
        {}

        ~HPRecordGuard()
        {
            m_ctx.RetireHPRecord(m_record);
        }

        HPRecord *Ptr() const
        {
            return m_record;
        }
    };

    std::atomic<HPRecord*>       m_head;
    std::atomic_int              m_H;

    TLSAllocation<HPRecordGuard> t_myhprec;

    static_assert(decltype(m_head)::is_always_lock_free);
    static_assert(decltype(m_H)::is_always_lock_free);

    HPContext(HPContext&&) = delete;
    HPContext(HPContext const&) = delete;
    HPContext& operator=(HPContext&&) = delete;
    HPContext& operator=(HPContext const&) = delete;

    void ReleaseHazard(int index);

    HPRecord *AllocateHPRecord();
    void RetireHPRecord(HPRecord *record);

    void Scan(HPRecord *head);
    void HelpScan();

public:

    using hazard_ptr_type = HazardPtr;

    HPContext()
        : m_head{}
        , m_H{}
        , t_myhprec{AllocTLS<HPRecordGuard>()}
    {
        /* Force the pointer to get placed on the 
         * thread destructors stack immediately,
         * such that the TLS of HPRecord is guaranteed
         * to be destroyed before the TLS of any object
         * which makes use of it.
         */
        std::ignore = t_myhprec.GetThreadSpecific(*this);
    }

    ~HPContext();

    [[nodiscard]] hazard_ptr_type AddHazard(int index, NodeType *node);
    void RetireHazard(NodeType *node);
};

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::ReleaseHazard(int index)
{
    auto rec = t_myhprec.GetThreadSpecific(*this);
    rec->Ptr()->m_hp[index].store(nullptr, std::memory_order_release);
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
typename HPContext<NodeType, K, R>::HPRecord *HPContext<NodeType, K, R>::AllocateHPRecord()
{
    /* First try to use a retired HP record */
    HPRecord *hprec;
    for(hprec = m_head.load(std::memory_order_acquire); 
        hprec; 
        hprec = hprec->m_next.load(std::memory_order_acquire)) {

        if(hprec->m_active.load(std::memory_order_acquire))
            continue;
        bool expected = false;
        if(!hprec->m_active.compare_exchange_weak(expected, true,
            std::memory_order_release, std::memory_order_relaxed))
            continue;
        /* Succeeded in locking an inactive HP record */
        return hprec;
    }

    /* No HP avaiable for reuse. Increment H, then allocate 
     * a new HP and push it.
     */
    int oldcount = m_H.load(std::memory_order_relaxed);
    while(!m_H.compare_exchange_weak(oldcount, oldcount + K,
        std::memory_order_release, std::memory_order_relaxed));

    /* Allocate and push a new HPRecord */
    hprec = new HPRecord{};
    hprec->m_active.store(true, std::memory_order_release);

    HPRecord *oldhead = m_head.load(std::memory_order_relaxed);
    do{
        hprec->m_next.store(oldhead, std::memory_order_release);
    }while(!m_head.compare_exchange_weak(oldhead, hprec, 
        std::memory_order_release, std::memory_order_relaxed));

    return hprec;
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::RetireHPRecord(HPRecord *record)
{
    for(int i = 0; i < K; i++)
        record->m_hp[i].store(nullptr, std::memory_order_release);
    record->m_active.store(false, std::memory_order_release);
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::Scan(HPRecord *head)
{
    /* Stage 1: Scan HP list and insert non-null values in plist */
    std::vector<NodeType*> plist;
    HPRecord *hprec = m_head.load(std::memory_order_acquire);
    while(hprec) {
        for(int i = 0; i < K; i++) {
            NodeType *hptr = hprec->m_hp[i].load(std::memory_order_acquire);
            if(hptr)
                plist.push_back(hptr);
        }
        hprec = hprec->m_next.load(std::memory_order_acquire);
    }
    std::sort(plist.begin(), plist.end());

    /* Stage 2: Search plist */
    HPRecord *myrec = t_myhprec.GetThreadSpecific(*this)->Ptr();
    std::vector<NodeType*> tmplist = std::move(myrec->m_rlist);
    myrec->m_rlist.clear();
    myrec->m_rcount = 0;

    auto node = tmplist.cbegin();
    while(node != tmplist.cend()) {
        if(std::binary_search(plist.begin(), plist.end(), *node)) {
            myrec->m_rlist.push_back(*node);
            myrec->m_rcount++;
        }else{
            delete *node;
        }
        node++;
    }
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::HelpScan()
{
    HPRecord *hprec;
    for(hprec = m_head.load(std::memory_order_acquire); 
        hprec; 
        hprec = hprec->m_next.load(std::memory_order_acquire)) {

        if(hprec->m_active.load(std::memory_order_acquire))
            continue;

        /* Acquire-Release ordering is required here to guaranteed 
         * that changes to rlist from another thread running HelpScan
         * are visible.
         */
        bool expected = false;
        if(!hprec->m_active.compare_exchange_weak(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed))
            continue;

        auto it = hprec->m_rlist.cbegin();
        for(; it != hprec->m_rlist.cend(); it++) {

            NodeType *node = *it;
            HPRecord *myrec = t_myhprec.GetThreadSpecific(*this)->Ptr();
            myrec->m_rlist.push_back(node);
            myrec->m_rcount++;

            if(myrec->m_rcount >= R)
                Scan(m_head.load(std::memory_order_relaxed));
        }
        hprec->m_rlist.clear();
        hprec->m_rcount = 0;

        hprec->m_active.store(false, std::memory_order_release);
    }
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
HPContext<NodeType, K, R>::~HPContext()
{
    t_myhprec.ClearAllThreadSpecific();

    HPRecord *hprec = m_head.load(std::memory_order_acquire);
    while(hprec) {

        bool active;
        Backoff backoff{10, 1'000, 10'000};
        do{
            active = hprec->m_active.load(std::memory_order_acquire);
            if(!active)
                backoff.BackoffMaybe();
        }while(active && !backoff.TimedOut());
        if(active) {
            /* This is an extraordinary circumstance such
             * as an untimely hung thread - do our best to
             * be robust and keep going.
             */
            delete hprec;
            break;
        }

        auto it = hprec->m_rlist.cbegin();
        for(; it != hprec->m_rlist.cend(); it++) {
            if(*it)
                delete *it;
        }
        HPRecord *old = hprec;
        hprec = hprec->m_next.load(std::memory_order_acquire);
        delete old;
    }
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
[[nodiscard]] typename HPContext<NodeType, K, R>::hazard_ptr_type 
HPContext<NodeType, K, R>::AddHazard(int index, NodeType *node)
{
    if(index >= K) [[unlikely]]
        throw std::out_of_range{"Hazard index out of range."};

    auto rec = t_myhprec.GetThreadSpecific(*this);
    rec->Ptr()->m_hp[index].store(node, std::memory_order_release);
    return {node, index, *this};
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::RetireHazard(NodeType *node)
{
    HPRecord *myrec = t_myhprec.GetThreadSpecific(*this)->Ptr();
    myrec->m_rlist.push_back(node);
    myrec->m_rcount++;

    HPRecord *head = m_head.load(std::memory_order_relaxed);
    if(myrec->m_rcount >= R) {
        Scan(head);
        HelpScan();
    }
}

export 
template <typename NodeType, std::size_t K, std::size_t R>
using HazardPtr = typename HPContext<NodeType, K, R>::hazard_ptr_type;

} //namespace pe

