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

export module atomic_struct;

import atomic_work;
import shared_ptr;
import assert;
import logger;

import <optional>;
import <array>;
import <atomic>;
import <variant>;
import <type_traits>;

namespace pe{

/* 
 * An arbitrary-sized atomic.
 */
export
template <typename T>
requires (std::is_standard_layout_v<T> && std::is_trivial_v<T>)
class AtomicStruct
{
private:

    constexpr static std::size_t kNumWords = (sizeof(T) + (sizeof(uint32_t) - 1))/ sizeof(uint32_t);
    constexpr static std::size_t kNumHalfWords = kNumWords * 2;

    struct alignas(8) SequencedHalfWord
    {
        uint32_t                 m_seqnum;
        std::array<std::byte, 2> m_curr_bytes;
        std::array<std::byte, 2> m_prev_bytes;
    };

    using AtomicSequencedHalfWord = std::atomic<SequencedHalfWord>;
    static_assert(AtomicSequencedHalfWord::is_always_lock_free);

    using SequencedDataArray = std::array<AtomicSequencedHalfWord, kNumHalfWords>;

    struct LoadRequest
    {
        pe::shared_ptr<pe::atomic_shared_ptr<T>>  m_out;
        SequencedDataArray&                       m_data;

        LoadRequest(decltype(m_out) out, decltype(m_data) data)
            : m_out{out}
            , m_data{data}
        {}
    };

    struct StoreRequest
    {
        T                   m_desired;
        SequencedDataArray& m_data;

        StoreRequest(decltype(m_desired) desired, decltype(m_data) data)
            : m_desired{desired}
            , m_data{data}
        {}
    };

    struct ExchangeRequest
    {
        T                                        m_desired;
        pe::shared_ptr<pe::atomic_shared_ptr<T>> m_out;
        SequencedDataArray&                      m_data;

        ExchangeRequest(decltype(m_desired) desired, decltype(m_out) out, decltype(m_data) data)
            : m_desired{desired}
            , m_out{out}
            , m_data{data}
        {}
    };

    struct CompareExchangeResult
    {
        T    m_expected;
        bool m_result;
    };

    struct CompareExchangeRequest
    {
        using ResultSharedPtr = pe::shared_ptr<pe::atomic_shared_ptr<CompareExchangeResult>>;

        T                   m_expected;
        T                   m_desired;
        ResultSharedPtr     m_out;
        SequencedDataArray& m_data;

        CompareExchangeRequest(decltype(m_expected) expected, decltype(m_desired) desired,
            decltype(m_out) out, decltype(m_data) data)
            : m_expected{expected}
            , m_desired{desired}
            , m_out{out}
            , m_data{data}
        {}
    };

    struct Request
    {
        enum class Type
        {
            eLoad,
            eStore,
            eExchange,
            eCompareExchange
        };

        using arg_type = std::variant<LoadRequest, StoreRequest, 
            ExchangeRequest, CompareExchangeRequest>;

        static inline std::atomic_uint32_t s_next_version{};

        uint32_t m_version;
        Type     m_type;
        arg_type m_arg;

        template <typename RequestType, typename... Args>
        Request(Type type, std::in_place_type_t<RequestType> reqtype, Args&&... args)
            : m_version{s_next_version.fetch_add(1, std::memory_order_relaxed)}
            , m_type{type}
            , m_arg{reqtype, std::forward<Args>(args)...}
        {}

        uint32_t Version() const
        {
            return m_version;
        }
    };

    AtomicStatefulSerialWork<Request> m_work;
    SequencedDataArray                m_sequenced_data;

    static inline pe::shared_ptr<void> s_consumed_marker = pe::static_pointer_cast<void>(
        pe::make_shared<std::monostate>()
    );

    static bool seqnum_passed(uint32_t a, uint32_t b)
    {
        return (static_cast<int32_t>((b) - (a)) < 0);
    }

    static void process_request(Request *request, uint32_t seqnum)
    {
        switch(request->m_type) {
        case Request::Type::eLoad: {

            const auto& arg = std::get<LoadRequest>(request->m_arg);
            auto ptr = pe::make_shared<T>();
            auto buffer = reinterpret_cast<std::byte*>(ptr.get());

            for(int i = 0; i < kNumHalfWords; i++) {

                SequencedHalfWord word = arg.m_data[i].load(std::memory_order_relaxed);

                while(true) {
                    if(seqnum_passed(word.m_seqnum, seqnum))
                        return; /* this is a lagging request */
                    SequencedHalfWord new_word{seqnum, word.m_curr_bytes, word.m_prev_bytes};
                    if(arg.m_data[i].compare_exchange_strong(word, new_word,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }

                if(i == kNumHalfWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                    std::memcpy(buffer + (i * 2), std::begin(word.m_curr_bytes), num_trailing_bytes);
                }else{
                    std::memcpy(buffer + (i * 2), std::begin(word.m_curr_bytes), 2);
                }
            }

            auto curr = arg.m_out->load(std::memory_order_relaxed);
            while(!curr) {
                arg.m_out->compare_exchange_strong(curr, ptr,
                    std::memory_order_release, std::memory_order_relaxed);
            }
            break;
        }
        case Request::Type::eStore: {

            auto& arg = std::get<StoreRequest>(request->m_arg);
            auto buffer = reinterpret_cast<std::byte*>(&arg.m_desired);

            for(int i = 0; i < kNumHalfWords; i++) {

                SequencedHalfWord word = arg.m_data[i].load(std::memory_order_relaxed);
                SequencedHalfWord new_word{seqnum};

                if(i == kNumHalfWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                    std::memcpy(std::begin(new_word.m_curr_bytes), buffer + (i * 2),
                        num_trailing_bytes);
                }else{
                    std::memcpy(std::begin(new_word.m_curr_bytes), buffer + (i * 2), 2);
                }

                /* We don't need to update the previous bytes for an unconditional store */
                std::memcpy(std::begin(new_word.m_prev_bytes), std::begin(word.m_prev_bytes), 2);
                while(true) {
                    if(seqnum_passed(word.m_seqnum, seqnum))
                        return; /* this is a lagging request */
                    if(arg.m_data[i].compare_exchange_strong(word, new_word,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }
            }
            break;
        }
        case Request::Type::eExchange: {

            auto& arg = std::get<ExchangeRequest>(request->m_arg);
            auto ptr = pe::make_shared<T>();
            auto out_buffer = reinterpret_cast<std::byte*>(ptr.get());
            auto in_buffer = reinterpret_cast<std::byte*>(&arg.m_desired);

            for(int i = 0; i < kNumHalfWords; i++) {

                SequencedHalfWord word = arg.m_data[i].load(std::memory_order_relaxed);
                SequencedHalfWord new_word{seqnum};

                if(i == kNumHalfWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                    if(word.m_seqnum != seqnum) {
                        /* This half-word has not yet been updated by a concurrent request */
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_curr_bytes), 2);
                    }else{
                        /* This half-word has already been updated by a concurrent request */
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_prev_bytes), num_trailing_bytes);
                    }
                    std::memcpy(std::begin(new_word.m_curr_bytes), in_buffer + (i * 2),
                        num_trailing_bytes);
                }else{
                    if(word.m_seqnum != seqnum) {
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_curr_bytes), 2);
                    }else{
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_prev_bytes), 2);
                    }
                    std::memcpy(std::begin(new_word.m_curr_bytes), in_buffer + (i * 2), 2);
                }

                while(true) {
                    if(seqnum_passed(word.m_seqnum, seqnum))
                        return; /* this is a lagging request */
                    if(arg.m_data[i].compare_exchange_strong(word, new_word,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }
                auto old_bytes = (word.m_seqnum == seqnum) ? std::begin(word.m_prev_bytes)
                                                           : std::begin(word.m_curr_bytes);
                if(i == kNumHalfWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                    std::memcpy(out_buffer + (i * 2), old_bytes, num_trailing_bytes);
                }else{
                    std::memcpy(out_buffer + (i * 2), old_bytes, 2);
                }
            }

            auto curr = arg.m_out->load(std::memory_order_relaxed);
            while(!curr) {
                arg.m_out->compare_exchange_strong(curr, ptr,
                    std::memory_order_release, std::memory_order_relaxed);
            }
            break;
        }
        case Request::Type::eCompareExchange: {

            auto& arg = std::get<CompareExchangeRequest>(request->m_arg);
            auto ptr = pe::make_shared<CompareExchangeResult>();

            T read{};
            auto in_buffer = reinterpret_cast<std::byte*>(&arg.m_desired);
            auto out_buffer = reinterpret_cast<std::byte*>(&read);

            /* First read the entire buffer. The update should only take
             * place if the contents of the buffer match 'expected'.
             */
            for(int i = 0; i < kNumHalfWords; i++) {

                SequencedHalfWord word = arg.m_data[i].load(std::memory_order_relaxed);
                if(seqnum_passed(word.m_seqnum, seqnum))
                    return; /* this is a lagging request */

                if(word.m_seqnum != seqnum) {
                    /* This half-word has not yet been updated by a concurrent request */
                    if(i == kNumHalfWords - 1) {
                        std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                        std::memcpy(out_buffer + (i * 2), std::begin(word.m_curr_bytes), 
                            num_trailing_bytes);
                    }else{
                        std::memcpy(out_buffer + (i * 2), std::begin(word.m_curr_bytes), 2);
                    }
                }else{
                    if(i == kNumHalfWords - 1) {
                        std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                        std::memcpy(out_buffer + (i * 2), std::begin(word.m_prev_bytes), 
                            num_trailing_bytes);
                    }else{
                        std::memcpy(out_buffer + (i * 2), std::begin(word.m_prev_bytes), 2);
                    }
                }
            }

            /* In case the 'expected' value does not match the value of
             * the buffer, the sequence numbers of the half words would
             * not be updated. While it is possible to go back for another
             * pass and bump the sequence numbers, there is no necessity
             * to do so.
             */
            if(std::memcmp(&read, &arg.m_expected, sizeof(T))) {
                ptr->m_result = false;
                ptr->m_expected = read;
                goto out;
            }

            /* Now we've established that the current value of the buffer
             * matches 'expected'. Overwrite the contents with 'desired'
             */
            for(int i = 0; i < kNumHalfWords; i++) {

                SequencedHalfWord word = arg.m_data[i].load(std::memory_order_relaxed);
                SequencedHalfWord new_word{seqnum};

                if(i == kNumHalfWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                    if(word.m_seqnum != seqnum) {
                        /* This half-word has not yet been updated by a concurrent request */
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_curr_bytes), 2);
                    }else{
                        /* This half-word has already been updated by a concurrent request */
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_prev_bytes), num_trailing_bytes);
                    }
                    std::memcpy(std::begin(new_word.m_curr_bytes), in_buffer + (i * 2),
                        num_trailing_bytes);
                }else{
                    if(word.m_seqnum != seqnum) {
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_curr_bytes), 2);
                    }else{
                        std::memcpy(std::begin(new_word.m_prev_bytes), 
                            std::begin(word.m_prev_bytes), 2);
                    }
                    std::memcpy(std::begin(new_word.m_curr_bytes), in_buffer + (i * 2), 2);
                }

                while(true) {
                    if(seqnum_passed(word.m_seqnum, seqnum))
                        return; /* this is a lagging request */
                    if(arg.m_data[i].compare_exchange_strong(word, new_word,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }
                auto old_bytes = (word.m_seqnum == seqnum) ? std::begin(word.m_prev_bytes)
                                                           : std::begin(word.m_curr_bytes);
                if(i == kNumHalfWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % 2) > 0) ? (sizeof(T) % 2) : 2;
                    std::memcpy(out_buffer + (i * 2), old_bytes, num_trailing_bytes);
                }else{
                    std::memcpy(out_buffer + (i * 2), old_bytes, 2);
                }
            }

            ptr->m_result = true;
            ptr->m_expected = arg.m_expected;

        out:
            auto curr = arg.m_out->load(std::memory_order_relaxed);
            while(!curr) {
                arg.m_out->compare_exchange_strong(curr, ptr,
                    std::memory_order_release, std::memory_order_relaxed);
            }

            break;
        }
        default:
            pe::assert(0);
        }
    }

public:

    AtomicStruct(T desired = T{})
        : m_work{}
        , m_sequenced_data{}
    {
        auto request = std::make_unique<Request>(Request::Type::eStore,
            std::in_place_type_t<StoreRequest>{}, desired, m_sequenced_data);
        m_work.PerformSerially(std::move(request), process_request, std::optional<int>{1});
    }

    T Load()
    {
        auto result = pe::make_shared<pe::atomic_shared_ptr<T>>();
        auto request = std::make_unique<Request>(Request::Type::eLoad,
            std::in_place_type_t<LoadRequest>{}, result, m_sequenced_data);

        m_work.PerformSerially(std::move(request), process_request);

        auto ret = *result->load(std::memory_order_acquire);
        result->store(pe::static_pointer_cast<T>(s_consumed_marker), std::memory_order_relaxed);
        return ret;
    }

    void Store(T desired)
    {
        auto request = std::make_unique<Request>(Request::Type::eStore,
            std::in_place_type_t<StoreRequest>{}, desired, m_sequenced_data);
        m_work.PerformSerially(std::move(request), process_request);
    }

    T Exchange(T desired)
    {
        auto result = pe::make_shared<pe::atomic_shared_ptr<T>>();
        auto request = std::make_unique<Request>(Request::Type::eExchange,
            std::in_place_type_t<ExchangeRequest>{}, desired, result, m_sequenced_data);

        m_work.PerformSerially(std::move(request), process_request);

        auto ret = *result->load(std::memory_order_acquire);
        result->store(pe::static_pointer_cast<T>(s_consumed_marker), std::memory_order_relaxed);
        return ret;
    }

    bool CompareExchange(T& expected, T desired)
    {
        auto result = pe::make_shared<pe::atomic_shared_ptr<CompareExchangeResult>>();
        auto request = std::make_unique<Request>(Request::Type::eCompareExchange,
            std::in_place_type_t<CompareExchangeRequest>{}, expected, desired, 
            result, m_sequenced_data);

        m_work.PerformSerially(std::move(request), process_request);

        auto retval = *result->load(std::memory_order_acquire);
        expected = retval.m_expected;
        result->store(pe::static_pointer_cast<CompareExchangeResult>(s_consumed_marker),
            std::memory_order_relaxed);
        return retval.m_result;
    }
};

}; //namespace pe

