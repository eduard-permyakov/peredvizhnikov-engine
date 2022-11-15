export module shared_ptr:base;

import platform;
import logger;
import meta;
import assert;
import concurrency;

import <memory>;
import <thread>;
import <string>;
import <vector>;
import <variant>;
import <mutex>;
import <unordered_map>;
import <typeinfo>;
import <iostream>;
import <sstream>;
import <iomanip>;
import <memory_resource>;

/*
 * Forward declarations
 */
namespace pe
{
    export template <typename T> class shared_ptr;
    export template <typename T> class weak_ptr;
    export template <typename T> class enable_shared_from_this;
    export template <typename T> class atomic_shared_ptr;

    export template <class Y, class U, class V>
    std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os,
                                         const shared_ptr<Y>& ptr);
};


namespace pe{

using instance_id_t = uint64_t;
inline std::atomic<instance_id_t> s_next_instance_id;

/* Descriptor of a single owner of a shared object, tracking
 * where the object was created. For debugging/tracing purposes.
 */
struct Owner
{
    instance_id_t            m_id;
    const void              *m_instance;
    std::string              m_typename;
    std::thread::id          m_thread;
    std::string              m_thread_name;
    std::vector<std::string> m_backtrace;

    bool operator==(const Owner& rhs) const noexcept
    {
        return m_id == rhs.m_id;
    }
};

using flag_type = std::conditional_t<kDebug, bool, std::monostate>;
using vector_type = std::conditional_t<kDebug, std::vector<Owner>, std::monostate>;
using lock_type = std::conditional_t<kDebug, std::mutex, std::monostate>;
using owner_type = std::conditional_t<kDebug, Owner, std::monostate>;

/* Type-erased deleter object.
 */
struct Deleter
{
    /* std::function performs small-size optimization - it
     * will not allocate heap memory if the deleter object
     * is small enough.
     */
    std::function<void(void*)> m_deleter;

    template <typename T>
    struct TypeEncoder{};

    template <typename Deleter>
    struct Wrapped
    {
        Deleter m_deleter;
        void  (*m_destroy)(Deleter& deleter, void*);

        template <typename T>
        Wrapped(TypeEncoder<T>, Deleter deleter)
            : m_deleter{deleter}
            , m_destroy(+[](Deleter& deleter, void *ptr){
                /* 
                 * For functors which provide overloads, we need 
                 * to cast the pointer to its' original type in
                 * order to select the appropriate overload. There
                 * is no way to statically deduce the appropriate
                 * overload but we don't want to add an extra 
                 * template parameter to Wrapped, as that will
                 * compilcate the 'get_deleter' logic.
                 */
                deleter(static_cast<std::remove_extent_t<T>*>(ptr));
            })
        {}

        void operator()(void *ptr) noexcept
        {
            m_destroy(m_deleter, ptr);
        }
    };

    template <typename T, typename D>
    Deleter(TypeEncoder<T> type, D deleter)
        : m_deleter{Wrapped<D>(type, deleter)}
    {}

    Deleter(std::nullptr_t ptr)
        : m_deleter{ptr}
    {}

    void operator()(void *ptr) noexcept
    {
        m_deleter(ptr);
    }
};

/* Type-erased allocator object with small-size optimization.
 */
struct Allocator
{
    struct AllocatorInterface
    {
        virtual void move_to(void *ptr) = 0;
        virtual std::size_t block_size() = 0;
        virtual void *allocate(std::size_t n) = 0;
        virtual void deallocate(void *ptr, std::size_t n) = 0;
        virtual ~AllocatorInterface() = default;
    };

    AllocatorInterface *m_allocator;
    std::byte           m_small_allocator[16];
    void              (*m_destructor)(AllocatorInterface*);

    template <typename Allocator, std::size_t BlockSize>
    struct Wrapped : AllocatorInterface
    {
        Allocator m_allocator;

        Wrapped(Allocator allocator)
            : m_allocator{allocator}
        {}

        Wrapped(Wrapped&& other)
            : m_allocator{std::move(other.m_allocator)}
        {}

        virtual constexpr std::size_t block_size()
        {
            return BlockSize;
        }

        virtual void move_to(void *ptr)
        {
            new (ptr) Wrapped(std::move(*this));
        }

        virtual void *allocate(std::size_t n)
        {
            return m_allocator.allocate(n);
        }

        virtual void deallocate(void *ptr, std::size_t n)
        {
            m_allocator.deallocate(static_cast<typename Allocator::value_type*>(ptr), n);
        }
    };

    template <typename Alloc, typename BlockSize>
    requires requires(std::size_t n, Alloc allocator, typename Alloc::value_type *ptr)
    {
        {allocator.allocate(n)} -> std::same_as<typename Alloc::value_type*>;
        {allocator.deallocate(ptr, n)} -> std::same_as<void>;
    }
    Allocator(Alloc allocator, BlockSize size)
    {
        if(sizeof(Wrapped<Alloc, BlockSize::value>) > sizeof(m_small_allocator)) {
            m_allocator = new Wrapped<Alloc, BlockSize::value>{allocator};
            m_destructor = +[](AllocatorInterface *ptr) {
                delete ptr;
            };
        }else{
            new (m_small_allocator) Wrapped<Alloc, BlockSize::value>{allocator};
            m_allocator = std::launder(
                reinterpret_cast<Wrapped<Alloc, BlockSize::value>*>(m_small_allocator));
            m_destructor = +[](AllocatorInterface *ptr) {
                ptr->~AllocatorInterface();
            };
        }
    }

    Allocator(Allocator const&) = delete;
    Allocator& operator=(Allocator const&) = delete;
    Allocator& operator=(Allocator&& other) = delete;

    Allocator(Allocator&& other)
    {
        if(other.m_allocator == reinterpret_cast<AllocatorInterface*>(other.m_small_allocator)) {
            other.m_allocator->move_to(m_small_allocator);
            m_allocator = std::launder(reinterpret_cast<AllocatorInterface*>(m_small_allocator));
        }else{
            m_allocator = other.m_allocator;
        }
        m_destructor = other.m_destructor;
        other.m_allocator = nullptr;
        other.m_destructor = nullptr;
    }

    Allocator(std::nullptr_t ptr)
        : m_allocator{nullptr}
        , m_small_allocator{}
        , m_destructor{nullptr}
    {}

    ~Allocator()
    {
        if(m_destructor) {
            m_destructor(m_allocator);
        }
    }

    void *allocate(std::size_t n)
    {
        return m_allocator->allocate(n);
    }

    void deallocate(void *ptr, std::size_t n)
    {
        return m_allocator->deallocate(ptr, n);
    }

    std::size_t block_size()
    {
        return m_allocator->block_size();
    }
};

struct alignas(16) SplitRefcount
{
    uint64_t m_basic_refcount;
    uint64_t m_strong_refcount;

    bool operator==(const SplitRefcount& rhs) const noexcept
    {
        return (m_basic_refcount == rhs.m_basic_refcount)
            && (m_strong_refcount == rhs.m_strong_refcount);
    }
};

using AtomicSplitRefcount = DoubleQuadWordAtomic<SplitRefcount>;

/* Align the control block to cache-line size. Eliminate all 
 * false sharing at the expense of wasting some memory.
 * Furthermore, we avoid the optimization of concatenating
 * the control block and the object in a single allocation
 * in our implementation. This way, the control block can be
 * allocated from a memory pool of fixed-sized objects.
 */
struct alignas(kCacheLineSize) ControlBlock
{
    /* For alignment reasons, place the deleter and allocator
     * first. This ensures no wasted space due to padding.
     */
    Deleter               m_deleter;
    Allocator             m_allocator;
    AtomicSplitRefcount   m_split_refcount;
    void                 *m_obj;
    std::atomic_uint32_t  m_weak_refcount;
    /* Debug state that isn't compiled in for release builds 
     */
    [[no_unique_address]] vector_type m_owners;
    [[no_unique_address]] lock_type   m_owners_lock;

    inline void inc_basic_refcount()
    {
        m_split_refcount.FetchAdd(1, 0, std::memory_order_relaxed);
    }

    inline void dec_basic_refcount()
    {
        /* The basic refcount can underflow and go negative if we have 
         * some outstanding strong references that have not yet transferred 
         * their cached local refcounts. This is not a problem since the
         * refcount will be normalized when the strong references are
         * destroyed.
         */
        if(m_split_refcount.FetchAdd(-1, 0, 
            std::memory_order_relaxed) == SplitRefcount{1, 0}) {

            m_deleter(m_obj);
            dec_weak_refcount();
        }
    }

    inline void inc_weak_refcount()
    {
        m_weak_refcount.fetch_add(1, std::memory_order_relaxed);
    }

    inline void dec_weak_refcount()
    {
        if(m_weak_refcount.fetch_sub(1, std::memory_order_relaxed) == 1) {

            auto allocator = std::move(m_allocator);
            std::destroy_at(this);
            allocator.deallocate(this, allocator.block_size());
        }
    }

    inline void inc_strong_refcount()
    {
        m_split_refcount.FetchAdd(0, 1, std::memory_order_relaxed);
    }

    void dec_strong_refcount(uint64_t basic_cached)
    {
        if(m_split_refcount.FetchAdd(basic_cached, -1,
            std::memory_order_relaxed) == SplitRefcount{-basic_cached, 1}) {

            m_deleter(m_obj);
            dec_weak_refcount();
        }
    }
};

static_assert(!kDebug ? (sizeof(ControlBlock) == kCacheLineSize) : true);

template <typename T, bool Debug = kDebug>
struct OwnershipLogger;

template <typename T, bool Debug = kDebug>
struct OwnershipTracer;

/*
 * Logging/tracing implementations used for release builds.
 */
template <typename T>
struct OwnershipLogger<T, false>
{
    static inline void log_newline() {}
    static inline void log_pointer(const shared_ptr<T>&, std::size_t, std::string_view) {}
    static inline void log_atomic_pointer(const atomic_shared_ptr<T>&,
        const atomic_shared_ptr<T>::State&, std::size_t, std::string_view) {}
    static inline void log_owner(const std::monostate&, std::string_view, bool) {}
    static inline void log_owners(const shared_ptr<T>&, std::size_t, std::monostate*) {}
};

template <typename T>
struct OwnershipTracer<T, false>
{
    template <typename Lock>
    static std::monostate lock_owners(Lock& lock) { return {}; }

    template <typename Owners>
    static inline std::size_t nowners(Owners& owners) { return 0; }

    template <typename Owners>
    static inline std::monostate *owners(Owners& owners) { return nullptr; }

    template <typename Owners>
    static inline void trace_add_owner(Owners&, std::monostate) {}

    template <typename Owners>
    static inline void trace_remove_owner(Owners&, std::monostate) {}
};

/*
 * Logging/tracing implementations used for debug builds.
 */
template <typename T>
struct OwnershipLogger<T, true>
{
    static inline void log_newline()
    {
        pe::log_ex(std::cout, nullptr, pe::TextColor::eWhite, "", true, true);
    }

    static inline void log_pointer(const shared_ptr<T>& ptr, std::size_t nowners,
        std::string_view action)
    {
        pe::log_ex(std::cout, nullptr, pe::TextColor::eGreen, "", true, true,
            "shared_ptr<" + ptr.m_owner.m_typename + "> ",
            "[", ptr.m_obj, "] ",
            "(Total Owners: ", nowners, ")",
            action.size() ? " " : "", action, ":");
    }

    static inline void log_atomic_pointer(const atomic_shared_ptr<T>& p,
        const atomic_shared_ptr<T>::State &state, 
        std::size_t nowners, std::string_view action)
    {
        pe::log_ex(std::cout, nullptr, pe::TextColor::eGreen, "", true, true,
            "atomic_shared_ptr<" + p.m_owner.m_typename + "> ",
            "[", p.ptr(state.m_control_block->m_obj, state.m_offset), "] ",
            "(Total Owners: ", nowners, ")",
            action.size() ? " " : "", action, ":");
    }

    static inline void log_owner(const Owner& owner, std::string_view heading, bool last)
    {
        pe::log_ex(std::cout, nullptr, pe::TextColor::eWhite, "", true, true, "|");

        std::stringstream stream;
        stream << "+--(" << heading << ") ";
        pe::log_ex(std::cout, nullptr, pe::TextColor::eWhite, "", true, false, stream.str());

        stream.str(std::string{});
        stream.clear();
        stream << std::hex << owner.m_thread;

        pe::log_ex(std::cout, nullptr, pe::TextColor::eYellow, "", false, true,
            "thread ", owner.m_thread_name, 
            " [0x", stream.str(), "]:",
            " [Instance: 0x", owner.m_instance, "]");

        for(auto& string : owner.m_backtrace) {
            pe::log_ex(std::cout, nullptr, pe::TextColor::eWhite, "", true, true,
                (last ? " " : "|"), "          ", string);
        }
    }

    static inline void log_owners(const shared_ptr<T> &ptr, std::size_t nowners, Owner *owners)
    {
        if(nowners < 0)
            return;

        log_pointer(ptr, nowners, "");
        for(int idx = 0; idx < nowners; idx++) {

            std::stringstream stream;
            stream << std::setfill('0') << std::setw(2) << idx + 1;

            bool last = (idx == nowners - 1);
            log_owner(owners[idx], stream.str(), last);
        }
    }
};

template <typename T>
struct OwnershipTracer<T, true>
{
    template <typename Lock>
    static std::lock_guard<Lock> lock_owners(Lock& lock)
    {
        return std::lock_guard<Lock>(lock);
    }

    template <typename Owners>
    static inline std::size_t nowners(Owners& owners)
    {
        return owners.size();
    }

    template <typename Owners>
    static inline Owner *owners(Owners& owners)
    {
        return owners.data();
    }

    template <typename Owners>
    static inline void trace_add_owner(Owners& owners, Owner owner)
    {
        auto it = std::find(owners.begin(), owners.end(), owner);
        pe::assert(it == owners.end());
        owners.push_back(owner);
    }

    template <typename Owners>
    static inline void trace_remove_owner(Owners& owners, Owner owner)
    {
        auto it = std::find(owners.begin(), owners.end(), owner);
        pe::assert(it != owners.end());
        owners.erase(it);
    }
};

/* shared_ptr implementation
 */
template <typename T>
class shared_ptr
{
private:

    template <typename Y>
    friend class shared_ptr;

    template <typename Y>
    friend class weak_ptr;

    template <typename Y>
    friend class atomic_shared_ptr;

    template <class Y>
    friend class enable_shared_from_this;

    template <typename Y, bool Debug>
    friend struct OwnershipLogger;

    ControlBlock                 *m_control_block;
    std::remove_extent_t<T>      *m_obj;

    /* Debug state that isn't compiled in for release builds */
    [[no_unique_address]] flag_type  m_tracing;
    [[no_unique_address]] flag_type  m_logging;
    [[no_unique_address]] owner_type m_owner;

    template <bool Debug = kDebug>
    requires (Debug == true)
    owner_type create_owner(flag_type tracing) const
    {
        if(tracing == flag_type{}) [[likely]]
            return {};

        std::string name = typeid(element_type).name();
        auto demangled = Demangle(name);
        if(demangled) {
            name = std::string{demangled.get()};
        }

        return {
            .m_id = s_next_instance_id.fetch_add(1, std::memory_order_relaxed),
            .m_instance = this,
            .m_typename = name,
            .m_thread = std::this_thread::get_id(),
            .m_thread_name = GetThreadName(),
            .m_backtrace = Backtrace()
        };
    }

    template <bool Debug = kDebug>
    requires (Debug == false)
    owner_type create_owner(flag_type tracing) const
    {
        return {};
    }

    inline void trace_create(flag_type from_weak = flag_type{}, 
        flag_type from_atomic = flag_type{})
    {
        if constexpr (!kDebug)
            return;

        if(!m_control_block || (m_tracing == flag_type{})) [[likely]]
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<T, kDebug>::lock_owners(m_control_block->m_owners_lock);
        OwnershipTracer<T, kDebug>::trace_add_owner(m_control_block->m_owners, m_owner);

        if(!m_control_block || (m_logging == flag_type{})) [[likely]]
            return;

        std::string op{"is created"};
        if(!(from_weak == flag_type{})) {
            op += " from a weak pointer";
        }
        if(!(from_atomic == flag_type{})) {
            op += " from an atomic pointer";
        }

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(m_control_block->m_owners);
        std::lock_guard<std::mutex> iolock{pe::iolock};
        OwnershipLogger<T, kDebug>::log_newline();
        OwnershipLogger<T, kDebug>::log_pointer(*this, nowners, op);
        OwnershipLogger<T, kDebug>::log_owner(m_owner, "new", true);
        OwnershipLogger<T, kDebug>::log_newline();
    }

    template <typename Y>
    inline void trace_move(const shared_ptr<Y>& from)
    {
        if constexpr (!kDebug)
            return;

        if(!from.m_control_block || (m_tracing == flag_type{})) [[likely]]
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<T, kDebug>::lock_owners(from.m_control_block->m_owners_lock);
        OwnershipTracer<Y, kDebug>::trace_add_owner(from.m_control_block->m_owners, 
            m_owner);
        OwnershipTracer<Y, kDebug>::trace_remove_owner(from.m_control_block->m_owners, 
            from.m_owner);

        if(m_logging == flag_type{}) [[likely]]
            return;

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(from.m_control_block->m_owners);
        std::lock_guard<std::mutex> iolock{pe::iolock};
        OwnershipLogger<Y, kDebug>::log_newline();
        OwnershipLogger<Y, kDebug>::log_pointer(from, nowners, "is moved");
        OwnershipLogger<Y, kDebug>::log_owner(from.m_owner, "src", false);
        OwnershipLogger<Y, kDebug>::log_owner(m_owner, "dst", true);
        OwnershipLogger<Y, kDebug>::log_newline();
    }

    template <typename Y>
    inline void trace_copy(const shared_ptr<Y>& from)
    {
        if constexpr (!kDebug)
            return;

        if(!from.m_control_block || (m_tracing == flag_type{})) [[likely]]
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<Y, kDebug>::lock_owners(from.m_control_block->m_owners_lock);
        OwnershipTracer<Y, kDebug>::trace_add_owner(from.m_control_block->m_owners, m_owner);

        if(m_logging == flag_type{}) [[likely]]
            return;

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(from.m_control_block->m_owners);
        std::lock_guard<std::mutex> iolock{pe::iolock};
        OwnershipLogger<Y, kDebug>::log_newline();
        OwnershipLogger<Y, kDebug>::log_pointer(from, nowners, "is copied");
        OwnershipLogger<Y, kDebug>::log_owner(from.m_owner, "src", false);
        OwnershipLogger<Y, kDebug>::log_owner(m_owner, "dst", true);
        OwnershipLogger<Y, kDebug>::log_newline();
    }

    inline void trace_clear()
    {
        if constexpr (!kDebug)
            return;

        if(!m_obj || (m_tracing == flag_type{})) [[likely]]
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<T, kDebug>::lock_owners(m_control_block->m_owners_lock);
        OwnershipTracer<T, kDebug>::trace_remove_owner(m_control_block->m_owners, m_owner);

        if(m_logging == flag_type{}) [[likely]]
            return;

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(m_control_block->m_owners);
        std::lock_guard<std::mutex> iolock{pe::iolock};
        OwnershipLogger<T, kDebug>::log_newline();
        OwnershipLogger<T, kDebug>::log_pointer(*this, nowners, "is reset");
        OwnershipLogger<T, kDebug>::log_owner(m_owner, "del", true);
        OwnershipLogger<T, kDebug>::log_newline();

        if(nowners == 0) {
            OwnershipLogger<T, kDebug>::log_newline();
            OwnershipLogger<T, kDebug>::log_pointer(*this, nowners, "is deleted");
            OwnershipLogger<T, kDebug>::log_owner(m_owner, "last", true);
            OwnershipLogger<T, kDebug>::log_newline();
        }
    }

    void clear()
    {
        m_control_block = nullptr;
        m_obj = nullptr;
    }

    void clear_flags()
    {
        m_tracing = {};
        m_logging = {};
    }

    inline long get_refcount() const
    {
        if(!m_control_block)
            return 0;
        return static_cast<int64_t>(m_control_block->m_split_refcount.LoadLower(
            std::memory_order_relaxed));
    }

    inline void inc_basic_refcount()
    {
        if(!m_control_block)
            return;
        m_control_block->inc_basic_refcount();
    }

    inline void dec_weak_refcount()
    {
        if(!m_control_block)
            return;
        m_control_block->dec_basic_refcount();
        clear();
    }

    inline void dec_basic_refcount()
    {
        if(!m_control_block)
            return;
        m_control_block->dec_basic_refcount();
        clear();
    }

    template <typename Y>
    explicit shared_ptr(ControlBlock *cb, Y *ptr, flag_type tracing, flag_type logging)
        : m_control_block{cb}
        , m_obj{ptr}
        , m_tracing{tracing}
        , m_logging{logging}
        , m_owner{create_owner(tracing)}
    {
        trace_create({}, true_value());
    }

public:

    template <bool Debug = kDebug>
    requires (Debug == true)
    static constexpr bool true_value()
    {
        return true;
    }

    template <bool Debug = kDebug>
    requires (Debug == false)
    static constexpr std::monostate true_value()
    {
        return {};
    }

    void LogOwners()
    {
        if constexpr (!kDebug)
            return;

        if(!m_control_block || (m_tracing == flag_type{}))
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<T, kDebug>::lock_owners(m_control_block->m_owners_lock);
        std::lock_guard<std::mutex> iolock{pe::iolock};

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(m_control_block->m_owners);
        owner_type *owners = OwnershipTracer<T, kDebug>::owners(m_control_block->m_owners);

        OwnershipLogger<T, kDebug>::log_newline();
        OwnershipLogger<T, kDebug>::log_owners(*this, nowners, owners);
        OwnershipLogger<T, kDebug>::log_newline();
    }

    using element_type = std::remove_extent_t<T>;
    using weak_type = weak_ptr<T>;

    constexpr shared_ptr(flag_type tracing = {}, flag_type logging = {}) noexcept
        : m_control_block{nullptr}
        , m_obj{nullptr}
        , m_tracing{}
        , m_logging{}
        , m_owner{create_owner(tracing)}
    {}

    constexpr shared_ptr(std::nullptr_t ptr,
        flag_type tracing = {}, flag_type logging = {}) noexcept
        : m_control_block{nullptr}
        , m_obj{ptr}
        , m_tracing{tracing}
        , m_logging{logging}
        , m_owner{create_owner(tracing)}
    {}

    template <class Y>
    explicit shared_ptr(Y *ptr, flag_type tracing = {}, flag_type logging = {})
        : shared_ptr(ptr, std::default_delete<T>{},
                     std::allocator<ControlBlock>{}, tracing, logging)
    {}

    template <class Y, class Deleter>
    shared_ptr(Y *ptr, Deleter d, flag_type tracing = {}, flag_type logging = {})
        : shared_ptr(ptr, d, std::allocator<ControlBlock>{}, tracing, logging)
    {}

    template<class Deleter>
    shared_ptr(std::nullptr_t ptr, Deleter d, flag_type tracing = {}, flag_type logging = {})
        : shared_ptr(ptr, d, std::allocator<ControlBlock>{}, tracing, logging)
    {}

    template <class Y, class Deleter, class Alloc>
    requires (std::is_same_v<typename Alloc::value_type, ControlBlock>)
    shared_ptr(Y *ptr, Deleter d, Alloc alloc, flag_type tracing = {}, flag_type logging = {})
        : m_control_block{alloc.allocate(1)}
        , m_obj{ptr}
        , m_tracing{tracing}
        , m_logging{logging}
        , m_owner{create_owner(tracing)}
    {
        new (m_control_block) ControlBlock{
            {pe::Deleter::TypeEncoder<Y>{}, d}, {alloc, std::integral_constant<std::size_t, 1>{}}, 
                {1u, 0u}, ptr, 1u};
        if constexpr (std::is_base_of_v<enable_shared_from_this<T>, Y>) {
            auto instance = static_cast<enable_shared_from_this<T>*>(ptr);
            instance->m_weak_this = weak_ptr<T>(*this);
        }
        trace_create();
    }

    template <class Y, class Deleter, class Alloc>
    requires (std::is_same_v<typename Alloc::value_type, std::byte>)
          || (std::is_same_v<typename Alloc::value_type, char>)
          || (std::is_same_v<typename Alloc::value_type, unsigned char>)
    shared_ptr(Y *ptr, Deleter d, Alloc alloc, flag_type tracing = {}, flag_type logging = {})
        : m_control_block{reinterpret_cast<ControlBlock*>(alloc.allocate(sizeof(ControlBlock)))}
        , m_obj{ptr}
        , m_tracing{tracing}
        , m_logging{logging}
        , m_owner{create_owner(tracing)}
    {
        new (m_control_block) ControlBlock{
            {pe::Deleter::TypeEncoder<Y>{}, d}, {alloc, 
                std::integral_constant<std::size_t, sizeof(ControlBlock)>{}}, {1u, 0u}, ptr, 1u};
        if constexpr (std::is_base_of_v<enable_shared_from_this<T>, Y>) {
            auto instance = static_cast<enable_shared_from_this<T>*>(ptr);
            instance->m_weak_this = weak_ptr<T>(*this);
        }
        trace_create();
    }

    template <class Deleter, class Alloc>
    shared_ptr(std::nullptr_t ptr, Deleter d, Alloc alloc,
        flag_type tracing = {}, flag_type logging = {})
        : shared_ptr(ptr, d, alloc, tracing, logging)
    {}

    template <class Y>
    shared_ptr(const shared_ptr<Y>& r, element_type* ptr) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{ptr}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        inc_basic_refcount();
        trace_copy(r);
    }

    template <class Y>
    shared_ptr(shared_ptr<Y>&& r, element_type* ptr) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{ptr}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        trace_move(r);
        r.clear();
    }

    shared_ptr(const shared_ptr& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        inc_basic_refcount();
        trace_copy(r);
    }

    template <class Y>
    shared_ptr(const shared_ptr<Y>& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        inc_basic_refcount();
        trace_copy(r);
    }

    shared_ptr(shared_ptr&& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        trace_move(r);
        r.clear();
    }

    template <class Y>
    shared_ptr(shared_ptr<Y>&& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        trace_move(r);
        r.clear();
    }

    template <class Y>
    explicit shared_ptr(const pe::weak_ptr<Y>& r)
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
        , m_owner{create_owner(r.m_tracing)}
    {
        if(r.expired()) {
            clear();
            throw std::bad_weak_ptr{};
        }
        inc_basic_refcount();
        trace_create(true_value());
    }

    template<class Y, class Deleter>
    shared_ptr(std::unique_ptr<Y, Deleter>&& r, flag_type tracing = {}, flag_type logging = {})
        : shared_ptr(r.get(), r.get_deleter(), std::allocator<ControlBlock>{}, tracing, logging)
    {
        r.release();
    }

    ~shared_ptr()
    {
        trace_clear();
        dec_basic_refcount();
    }

    shared_ptr& operator=(const shared_ptr& r) noexcept
    {
        if(this != &r) {
            trace_clear();
            trace_copy(r);
            shared_ptr tmp{r};
            tmp.clear_flags();
            swap(tmp);
        }
        return *this;
    }

    template <class Y>
    shared_ptr& operator=(const shared_ptr<Y>& r) noexcept
    {
        trace_clear();
        trace_copy(r);
        shared_ptr tmp{r};
        tmp.clear_flags();
        swap(tmp);
        return *this;
    }

    shared_ptr& operator=(shared_ptr&& r) noexcept
    {
        trace_clear();
        trace_move(r);
        swap(r);
        r.clear_flags();
        r.reset();
        return *this;
    }

    template <class Y>
    shared_ptr& operator=(shared_ptr<Y>&& r) noexcept
    {
        trace_clear();
        trace_move(r);
        shared_ptr tmp{std::move(r)};
        tmp.clear_flags();
        swap(tmp);
        return *this;
    }

    template <class Y, class Deleter>
    shared_ptr& operator=(std::unique_ptr<Y,Deleter>&& r)
    {
        trace_clear();
        shared_ptr tmp{r.get(), r.get_deleter()};
        swap(tmp);
        r.release();
        trace_create();
        return *this;
    }

    void reset() noexcept
    {
        trace_clear();
        shared_ptr tmp{nullptr};
        swap(tmp);
    }

    template <class Y>
    void reset(Y *ptr)
    {
        trace_clear();
        shared_ptr tmp{ptr, m_tracing, m_logging};
        swap(tmp);
    }

    template <class Y, class Deleter>
    void reset(Y *ptr, Deleter d)
    {
        trace_clear();
        shared_ptr tmp{ptr, d, m_tracing, m_logging};
        swap(tmp);
    }

    template <class Y, class Deleter, class Alloc>
    void reset(Y *ptr, Deleter d, Alloc alloc)
    {
        trace_clear();
        shared_ptr tmp{ptr, d, alloc, m_tracing, m_logging};
        swap(tmp);
    }

    void swap(shared_ptr& r) noexcept
    {
        std::swap(m_control_block, r.m_control_block);
        std::swap(m_obj, r.m_obj);
    }

    element_type *get() const noexcept
    {
        return m_obj;
    }

    template <typename U = T>
    requires (!std::is_void_v<U>)
    U& operator*() const noexcept
    {
        return *m_obj;
    }

    template <typename U = T>
    requires (!std::is_void_v<U>)
    U* operator->() const noexcept
    {
        return m_obj;
    }

    template <typename U = element_type>
    requires (!std::is_void_v<U>)
    U& operator[](std::ptrdiff_t idx) const
    {
        return m_obj[idx];
    }

    long use_count() const noexcept
    {
        return get_refcount();
    }

    explicit operator bool() const noexcept
    {
        return m_obj;
    }

    template <class Y>
    bool owner_before(const shared_ptr<Y>& other) const noexcept
    {
        return (m_control_block < other.m_control_block);
    }

    template <class Y>
    bool owner_before(const std::weak_ptr<Y>& other) const noexcept
    {
        return (m_control_block < other.m_control_block);
    }

    template <class U>
    bool operator==(const pe::shared_ptr<U>& rhs) const noexcept
    {
        return (m_obj == rhs.m_obj);
    }

    bool operator==(const std::nullptr_t rhs) const noexcept
    {
        return (m_obj == rhs);
    }

    template <class U>
    std::strong_ordering operator<=>(const shared_ptr<U>& rhs) const noexcept
    {
        return (m_obj <=> rhs.m_obj);
    }

    std::strong_ordering operator<=>(std::nullptr_t rhs) noexcept
    {
        return (m_obj <=> rhs);
    }

    template <class Deleter, class Y>
    friend Deleter* get_deleter(const shared_ptr<Y>& p) noexcept;

    template <class Y, class U, class V>
    friend std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os,
                                                const shared_ptr<Y>& ptr);
};

template <bool Log, bool Debug = kDebug>
struct flag_traits
{
    using flag_type = bool;
    static constexpr flag_type value = Log;
};

template <bool Log>
struct flag_traits<Log, false>
{
    using flag_type = std::monostate;
    static constexpr flag_type value = {};
};

template <bool Log>
inline constexpr typename flag_traits<Log>::flag_type flag_arg_v = flag_traits<Log>::value;

export
template <class T, bool Trace = false, bool Log = false, class... Args>
requires (!std::is_array_v<T>)
shared_ptr<T> make_shared(Args&&... args)
{
    T *obj = new T{std::forward<Args>(args)...};
    shared_ptr<T> ret{obj, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Trace = false, bool Log = false>
requires (std::is_array_v<T>)
shared_ptr<T> make_shared(std::size_t N)
{
    std::remove_extent_t<T> *obj = new std::remove_extent_t<T>[N];
    shared_ptr<T> ret{obj, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Trace = false, bool Log = false>
requires (!std::is_array_v<T>)
shared_ptr<T> make_shared()
{
    shared_ptr<T> ret{new T{}, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Trace = false, bool Log = false>
requires (std::is_array_v<T>)
shared_ptr<T> make_shared(std::size_t N, const std::remove_extent_t<T>& u)
{
    std::remove_extent_t<T> *obj = new std::remove_extent_t<T>[N]{u};
    shared_ptr<T> ret{obj, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Trace = false, bool Log = false>
requires (!std::is_array_v<T>)
shared_ptr<T> make_shared(const std::remove_extent_t<T>& u)
{
    T *obj = new T{u};
    shared_ptr<T> ret{obj, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, class Alloc, bool Trace = false, bool Log = false, class... Args>
requires (!std::is_array_v<T>)
shared_ptr<T> allocate_shared(const Alloc& alloc, Args&&... args)
{
    T *obj = new T{std::forward<Args>(args)...};
    shared_ptr<T> ret{obj, std::default_delete<T>{}, alloc, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, class Alloc, bool Trace = false, bool Log = false>
requires (std::is_array_v<T>)
shared_ptr<T> allocate_shared(const Alloc& alloc, std::size_t N)
{
    std::remove_extent_t<T> *obj = new std::remove_extent_t<T>[N];
    shared_ptr<T> ret{obj, std::default_delete<T>{}, alloc, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, class Alloc, bool Trace = false, bool Log = false>
requires (!std::is_array_v<T>)
shared_ptr<T> allocate_shared(const Alloc& alloc)
{
    T *obj = new T{};
    shared_ptr<T> ret{obj, std::default_delete<T>{}, alloc, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, class Alloc, bool Trace = false, bool Log = false>
requires (std::is_array_v<T>)
shared_ptr<T> allocate_shared(const Alloc& alloc, std::size_t N,
                              const std::remove_extent_t<T>& u)
{
    std::remove_extent_t<T> *obj = new std::remove_extent_t<T>[N]{u};
    shared_ptr<T> ret{obj, std::default_delete<T>{}, alloc, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, class Alloc, bool Trace = false, bool Log = false>
requires (!std::is_array_v<T>)
shared_ptr<T> allocate_shared(const Alloc& alloc,
                              const std::remove_extent_t<T>& u)
{
    T *obj = new T{};
    shared_ptr<T> ret{obj, std::default_delete<T>{}, alloc, flag_arg_v<Trace>, flag_arg_v<Log>};
    return ret;
}

export
template <class T, class U>
shared_ptr<T> static_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{ r, static_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> static_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{ r, static_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> dynamic_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{ r, dynamic_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> dynamic_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{ r, dynamic_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> const_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{ r, const_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> const_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{ r, const_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> reinterpret_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{ r, reinterpret_cast<T*>(r.get()) };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> reinterpret_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{ r, reinterpret_cast<T*>(r.get()) };
    return ret;
}

export
template <class Deleter, class T>
Deleter* get_deleter(const shared_ptr<T>& p) noexcept
{
    if(!p.m_control_block)
        return nullptr;

    using WrappedType = pe::Deleter::template Wrapped<Deleter>;
    WrappedType *wrapped = p.m_control_block->m_deleter.m_deleter.template target<WrappedType>();
    return &wrapped->m_deleter;
}

export
template <class T>
class enable_shared_from_this
{
private:

    template <typename Y>
    friend class shared_ptr;

    pe::weak_ptr<T> m_weak_this;

protected:

    constexpr enable_shared_from_this() noexcept
        : m_weak_this{}
    {}

    enable_shared_from_this(const enable_shared_from_this<T>& obj) noexcept
        : m_weak_this{}
    {}

    ~enable_shared_from_this() = default;

    enable_shared_from_this<T>& operator=(const enable_shared_from_this<T>& obj) noexcept
    {
        return *this;
    }

public:

    shared_ptr<T> shared_from_this()
    {
        return m_weak_this.lock();
    }

    shared_ptr<T const> shared_from_this() const
    {
        return m_weak_this.lock();
    }

    weak_ptr<T> weak_from_this() noexcept
    {
        return m_weak_this;
    }

    weak_ptr<T const> weak_from_this() const noexcept
    {
        return m_weak_this;
    }
};

template <typename T>
class weak_ptr
{
private:

    template <typename U>
    friend class shared_ptr;

    ControlBlock            *m_control_block;
    std::remove_extent_t<T> *m_obj;

    /* Debug state that isn't compiled in for release builds */
    [[no_unique_address]] flag_type m_tracing;
    [[no_unique_address]] flag_type m_logging;

    void clear()
    {
        m_control_block = nullptr;
        m_obj = nullptr;
    }

    inline long get_refcount() const
    {
        if(!m_control_block)
            return 0;
        return static_cast<int64_t>(m_control_block->m_split_refcount.LoadLower(
            std::memory_order_relaxed));
    }

    inline void inc_weak_refcount()
    {
        if(!m_control_block)
            return;
        m_control_block->inc_weak_refcount();
    }

    inline void dec_weak_refcount()
    {
        if(!m_control_block)
            return;

        m_control_block->dec_weak_refcount();
        clear();
    }

public:

    using element_type = std::remove_extent_t<T>;

    constexpr weak_ptr() noexcept
        : m_control_block{nullptr}
        , m_obj{nullptr}
        , m_tracing{}
        , m_logging{}
    {}

    weak_ptr(const weak_ptr& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
    {
        inc_weak_refcount();
    }

    template <class Y>
    weak_ptr(const weak_ptr<Y>& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
    {
        inc_weak_refcount();
    }

    template <class Y>
    weak_ptr(const shared_ptr<Y>& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
    {
        inc_weak_refcount();
    }

    weak_ptr(weak_ptr&& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
    {
        r.clear();
    }

    template <class Y>
    weak_ptr(weak_ptr<Y>&& r) noexcept
        : m_control_block{r.m_control_block}
        , m_obj{r.m_obj}
        , m_tracing{r.m_tracing}
        , m_logging{r.m_logging}
    {
        r.clear();
    }

    ~weak_ptr()
    {
        dec_weak_refcount();
    }

    weak_ptr& operator=(const weak_ptr& r) noexcept
    {
        if(this != &r) {
            weak_ptr tmp{r};
            swap(tmp);
        }
        return *this;
    }

    template<class Y>
    weak_ptr& operator=(const weak_ptr<Y>& r) noexcept
    {
        weak_ptr tmp{r};
        swap(tmp);
        return *this;
    }

    template<class Y>
    weak_ptr& operator=(const shared_ptr<Y>& r) noexcept
    {
        weak_ptr tmp{r};
        swap(tmp);
        return *this;
    }

    weak_ptr& operator=(weak_ptr&& r) noexcept
    {
        swap(r);
        r.reset();
        return *this;
    }

    template<class Y>
    weak_ptr& operator=(weak_ptr<Y>&& r) noexcept
    {
        weak_ptr tmp{std::move(r)};
        swap(tmp);
        return *this;
    }

    void reset() noexcept
    {
        weak_ptr tmp{};
        std::swap(m_control_block, tmp.m_control_block);
        std::swap(m_obj, tmp.m_obj);
    }

    void swap(weak_ptr& r) noexcept
    {
        std::swap(m_control_block, r.m_control_block);
        std::swap(m_obj, r.m_obj);
        std::swap(m_logging, r.m_logging);
        std::swap(m_tracing, r.m_tracing);
    }

    long use_count() const noexcept
    {
        return get_refcount();
    }

    bool expired() const noexcept
    {
        long refcount = get_refcount();
        return (refcount == 0);
    }

    shared_ptr<T> lock() const noexcept
    {
        if(expired()) {
            return shared_ptr<T>{};
        }else{
            return shared_ptr<T>{*this};
        }
    }

    template <class Y>
    bool owner_before(const weak_ptr<Y>& other) const noexcept
    {
        return (m_control_block < other.m_control_block);
    }

    template <class Y>
    bool owner_before(const std::shared_ptr<Y>& other) const noexcept
    {
        return (m_control_block < other.m_control_block);
    }
};

template <class T>
weak_ptr(shared_ptr<T>) -> weak_ptr<T>;

export template <class Y, class U, class V>
std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os,
                                     const shared_ptr<Y>& ptr)
{
    return (os << ptr.m_obj);
}

} // namespace pe

