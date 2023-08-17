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

    struct alignas(8) SequencedWord
    {
        uint32_t                 m_seqnum;
        std::array<std::byte, 4> m_bytes;
    };

    using AtomicSequencedWord = std::atomic<SequencedWord>;
    static_assert(AtomicSequencedWord::is_always_lock_free);

    using SequencedDataArray = std::array<AtomicSequencedWord, kNumWords>;

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
    };

    struct CompareAndSwapRequest
    {
        pe::atomic_shared_ptr<T>                     m_expected;
        T                                            m_desired;
        std::shared_ptr<pe::atomic_shared_ptr<bool>> m_out;
        SequencedDataArray&                          m_data;
    };

    struct Request
    {
        enum class Type
        {
            eLoad,
            eStore,
            eExchange,
            eCompareAndSwap
        };

        using arg_type = std::variant<LoadRequest, StoreRequest, 
            ExchangeRequest, CompareAndSwapRequest>;

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

            for(int i = 0; i < kNumWords; i++) {
                SequencedWord word = arg.m_data[i].load(std::memory_order_relaxed);
                while(true) {
                    if(seqnum_passed(word.m_seqnum, seqnum))
                        return; /* this is a lagging request */
                    SequencedWord new_word{seqnum, word.m_bytes};
                    if(arg.m_data[i].compare_exchange_strong(word, new_word,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }
                if(i == kNumWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % sizeof(uint32_t)) > 0)
                        ? (sizeof(T) % sizeof(uint32_t)) : sizeof(uint32_t);
                    std::memcpy(buffer + (i * sizeof(uint32_t)),
                        std::begin(word.m_bytes), num_trailing_bytes);
                }else{
                    std::memcpy(buffer + (i * sizeof(uint32_t)),
                        std::begin(word.m_bytes), sizeof(uint32_t));
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

            for(int i = 0; i < kNumWords; i++) {
                SequencedWord word = arg.m_data[i].load(std::memory_order_relaxed);
                SequencedWord new_word{seqnum};
                if(i == kNumWords - 1) {
                    std::size_t num_trailing_bytes = ((sizeof(T) % sizeof(uint32_t)) > 0)
                        ? (sizeof(T) % sizeof(uint32_t)) : sizeof(uint32_t);
                    std::memcpy(std::begin(new_word.m_bytes), buffer + (i * sizeof(uint32_t)),
                        num_trailing_bytes);
                }else{
                    std::memcpy(std::begin(new_word.m_bytes), buffer + (i * sizeof(uint32_t)),
                        sizeof(uint32_t));
                }
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
            break;
        }
        case Request::Type::eCompareAndSwap: {
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
        return *result->load(std::memory_order_acquire);
    }

    void Store(T desired)
    {
        auto request = std::make_unique<Request>(Request::Type::eStore,
            std::in_place_type_t<StoreRequest>{}, desired, m_sequenced_data);
        m_work.PerformSerially(std::move(request), process_request);
    }

    T Exchange(T desired);
    bool CompareExchange(T& expected, T desired);
};

}; //namespace pe

