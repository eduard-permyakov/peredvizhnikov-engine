export module shared_ptr;

import platform;
import logger;

export import <memory>;
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

/*
 * Forward declarations
 */
namespace pe
{
    export template <typename T> class shared_ptr;
    export template <typename T> class weak_ptr;
};

export
template <class T, class U, class V>
std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os, 
                                     const pe::shared_ptr<T>& ptr);

namespace pe{

/* 
 * An 'Owner' represents a single shared_ptr instance with some
 * data about where this instance was constructed. We manage a
 * global table keeping a list of all Owners that share ownership
 * of a single object.
 * 
 * Note that we cannot simply hash owners by the underlying raw
 * pointer as two distinct shared pointers can point to different
 * objects but still share ownership, like such:
 * 
 *   struct object {int x,y;};
 *   shared_ptr<object> o1 = make_shared<object>();
 *   shared_ptr<int>    o2(o1, &o1->y);
 *
 * In addition, it is possible for two distinct shared pointers to
 * point to the same object, but not share ownership, like such:
 *
 *   object o;
 *   shared_ptr<object> o1(&o, [](object *o){});
 *   shared_ptr<object> o2(&o, [](object *o){});
 *
 * std::shared_ptr provides a facility for checking if two pointers
 * share ownership with the 'owner_before' method. We can use that
 * to associate an 'ownership ID' which is equal for all instances 
 * sharing ownership. However, this means that we are required to 
 * compare each new instance with every existing instance to determine 
 * if they share ownership, which can become costly as the total number 
 * of tracked shared pointers climbs.
 */

using ownership_id_t = uint64_t;
using instance_id_t = uint64_t;

struct Owner
{
    std::weak_ptr<void>      m_raw;
    ownership_id_t           m_ownership_id;
    instance_id_t            m_instance_id;
    std::string              m_typename;
    std::thread::id          m_thread;
    std::string              m_thread_name;
    std::vector<std::string> m_backtrace;
};

using owners_map_type = std::unordered_map<ownership_id_t, std::vector<Owner>>;

inline std::mutex           s_owners_mutex{};
inline owners_map_type      s_owners{};
inline ownership_id_t       s_next_ownership_id{};
inline std::atomic_uint64_t s_next_instance_id{};

template <typename T>
[[maybe_unused]] ownership_id_t ownership_id(std::shared_ptr<T>& ptr)
{
    std::lock_guard<std::mutex> lock{s_owners_mutex};
    auto share_ownership = [](std::shared_ptr<T>& ptr, Owner& owner){
        auto& other = owner.m_raw;
        return !ptr.owner_before(other) && !other.owner_before(ptr);
    };

    for(auto& [key, owners] : s_owners) {
        for(auto& owner : owners) {
            if(share_ownership(ptr, owner))
                return key;
        }
    }
    return s_next_ownership_id++;
}

[[maybe_unused]] instance_id_t instance_id()
{
    uint64_t expected = s_next_instance_id.load(std::memory_order_relaxed);
    while(!s_next_instance_id.compare_exchange_weak(expected, expected + 1,
        std::memory_order_relaxed, std::memory_order_relaxed));
    return expected;
}

/*
 * Implementations used for release builds.
 */
template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void add_owner(std::monostate) {}

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void remove_owner(std::monostate) {}

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline std::size_t num_owners(std::monostate) { return {}; }

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void log_owner(std::monostate, std::string_view, bool) {}

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void log_owner(const Owner&, std::string_view, bool) {}

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void log_pointer(std::monostate, std::string_view) {}

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void log_pointer(const Owner&, std::string_view) {}

template <bool Debug = kDebug>
requires (Debug == false)
[[maybe_unused]] static inline void log_owners(std::monostate) {}

/*
 * Implementations used for debug builds.
 */
template <bool Debug = kDebug>
requires (Debug == true)
[[maybe_unused]] static inline void add_owner(Owner owner)
{
    ownership_id_t key = owner.m_ownership_id;
    s_owners[key].push_back(std::move(owner));
}

template <bool Debug = kDebug>
requires (Debug == true)
[[maybe_unused]] static inline void remove_owner(Owner owner)
{
    ownership_id_t key = owner.m_ownership_id;
    auto& owners = s_owners[key];
    auto it = std::find_if(std::begin(owners), std::end(owners), [&owner](Owner& other){
        return (owner.m_instance_id == other.m_instance_id);
    });
    owners.erase(it);
}

template <bool Debug = kDebug>
requires (Debug == true)
[[maybe_unused]] static inline std::size_t num_owners(const Owner& owner)
{
    return s_owners[owner.m_ownership_id].size();
}

template <bool Debug = kDebug>
requires (Debug == true)
[[maybe_unused]] static inline void log_owner(const Owner& owner, std::string_view heading, bool last)
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
        " [ID: 0x", owner.m_instance_id, "]");

    for(auto& string : owner.m_backtrace) {
        pe::log_ex(std::cout, nullptr, pe::TextColor::eWhite, "", true, true,
            (last ? " " : "|"), "          ", string);
    }
}

template <bool Debug = kDebug>
requires (Debug == true)
[[maybe_unused]] static inline void log_pointer(const Owner& owner, std::string_view action)
{
    std::stringstream stream;
    stream << std::hex << owner.m_ownership_id;

    pe::log_ex(std::cout, nullptr, pe::TextColor::eGreen, "", true, true,
        "shared_ptr<" + owner.m_typename + "> ",
        "[", owner.m_raw.lock(), "] ",
        "[Ownership ID: 0x", stream.str(), "] ",
        "(Total Owners: ", s_owners[owner.m_ownership_id].size(), ")",
        action.size() ? " " : "", action, ":");
}

template <bool Debug = kDebug>
requires (Debug == true)
[[maybe_unused]] static inline void log_owners(const Owner& owner)
{
    auto& owners = s_owners[owner.m_ownership_id];
    log_pointer(owner, "");

    int idx = 0;
    for(const auto& owner : owners) {

        std::stringstream stream;
        stream << std::setfill('0') << std::setw(2) << idx + 1;

        bool last = (idx == owners.size() - 1);
        log_owner(owner, stream.str(), last);

        idx++;
    }
}

[[maybe_unused]] static inline void log_newline()
{
    pe::log_ex(std::cout, nullptr, pe::TextColor::eWhite, "", true, true);
}

/* 
 * A wrapper around std::shared_ptr with additional debugging and 
 * tracing facilities.
 */
template <typename T>
class shared_ptr
{
private:

    using flag_type = std::conditional_t<kDebug, bool, std::monostate>;
    using owner_type = std::conditional_t<kDebug, Owner, std::monostate>;

    template <typename U>
    friend class weak_ptr;

    std::shared_ptr<T>                m_ptr;
    [[no_unique_address]] flag_type   m_logging;
    [[no_unique_address]] owner_type  m_owner;

    template <bool Debug = kDebug>
    requires (Debug == true)
    owner_type create_owner()
    {
        std::string name = typeid(element_type).name();
        auto demangled = Demangle(name);
        if(demangled) {
            name = std::string{demangled.get()};
        }

        return {
            .m_raw = m_ptr,
            .m_ownership_id = ownership_id(m_ptr),
            .m_instance_id = instance_id(),
            .m_typename = name,
            .m_thread = std::this_thread::get_id(),
            .m_thread_name = GetThreadName(),
            .m_backtrace = Backtrace()
        };
    }

    template <bool Debug = kDebug>
    requires (Debug == false)
    owner_type create_owner()
    {
        return {};
    }

    inline void trace_create()
    {
        if constexpr (!kDebug)
            return;

        std::lock_guard<std::mutex> l1{s_owners_mutex};
        add_owner(m_owner);

        if(m_logging == flag_type{})
            return;

        std::lock_guard<std::mutex> l2{iolock};
        log_newline();
        log_pointer(m_owner, "is created");
        log_owner(m_owner, "new", true);
        log_newline();
    }

    inline void trace_move(owner_type from)
    {
        if constexpr (!kDebug)
            return;

        std::lock_guard<std::mutex> l1{s_owners_mutex};
        remove_owner(from);
        add_owner(m_owner);

        if(m_logging == flag_type{})
            return;

        std::lock_guard<std::mutex> l2{iolock};
        log_newline();
        log_pointer(m_owner, "is moved");
        log_owner(from, "src", false);
        log_owner(m_owner, "dst", true);
        log_newline();
    }

    inline void trace_copy(owner_type from)
    {
        if constexpr (!kDebug)
            return;

        std::lock_guard<std::mutex> l1{s_owners_mutex};
        add_owner(m_owner);

        if(m_logging == flag_type{})
            return;

        std::lock_guard<std::mutex> l2{iolock};
        log_newline();
        log_pointer(m_owner, "is copied");
        log_owner(from, "src", false);
        log_owner(m_owner, "dst", true);
        log_newline();
    }

    inline void trace_clear()
    {
        if constexpr (!kDebug)
            return;

        std::lock_guard<std::mutex> l1{s_owners_mutex};
        remove_owner(m_owner);

        if(m_logging == flag_type{})
            return;

        std::lock_guard<std::mutex> l2{iolock};
        log_newline();
        log_pointer(m_owner, "is reset");
        log_owner(m_owner, "del", true);
        log_newline();

        if(num_owners(m_owner) == 0) {
            log_newline();
            log_pointer(m_owner, "is_deleted");
            log_owner(m_owner, "last", true);
            log_newline();
        }
    }

    template <typename U>
    requires (std::is_same_v<T, U>)
    explicit shared_ptr(std::shared_ptr<U>&& r, flag_type log = {}) noexcept
        : m_ptr{std::move(r)}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

public:

    void LogOwners()
    {
        if constexpr (!kDebug)
            return;

        std::lock_guard<std::mutex> l1{s_owners_mutex};
        std::lock_guard<std::mutex> l2{iolock};

        log_newline();
        log_owners(m_owner);
        log_newline();
    }

    using element_type = typename std::shared_ptr<T>::element_type;
    using weak_type = typename std::shared_ptr<T>::weak_type;

    constexpr shared_ptr(flag_type log = {}) noexcept
        : m_ptr{}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    constexpr shared_ptr(std::nullptr_t ptr, flag_type log = {}) noexcept
        : m_ptr{ptr}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template <class Y>
    explicit shared_ptr(Y *ptr, flag_type log = {})
        : m_ptr{ptr}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template <class Y, class Deleter>
    shared_ptr(Y *ptr, Deleter d, flag_type log = {})
        : m_ptr{ptr, d}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template<class Deleter>
    shared_ptr(std::nullptr_t ptr, Deleter d, flag_type log = {})
        : m_ptr{ptr, d}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template <class Y, class Deleter, class Alloc>
    shared_ptr(Y *ptr, Deleter d, Alloc alloc, flag_type log = {})
        : m_ptr{ptr, d, alloc}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template <class Deleter, class Alloc>
    shared_ptr(std::nullptr_t ptr, Deleter d, Alloc alloc, flag_type log = {})
        : m_ptr{ptr, d, alloc}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template <class Y>
    shared_ptr(const shared_ptr<Y>& r, element_type* ptr) noexcept
        : m_ptr{r.m_ptr, ptr}
        , m_logging{r.m_logging}
        , m_owner{create_owner()}
    {
        trace_copy(r.m_owner);
    }

    template <class Y>
    shared_ptr(shared_ptr<Y>&& r, element_type* ptr) noexcept
        : m_ptr{std::move(r.m_ptr), ptr}
        , m_logging{r.m_logging}
        , m_owner{create_owner()}
    {
        trace_move(r.m_owner);
    }

    shared_ptr(const shared_ptr& r) noexcept
        : m_ptr{r.m_ptr}
        , m_logging{r.m_logging}
        , m_owner{create_owner()}
    {
        trace_copy(r.m_owner);
    }

    template <class Y>
    shared_ptr(const shared_ptr<Y>& r) noexcept
        : m_ptr{r.m_ptr}
        , m_logging{r.m_logging}
        , m_owner{create_owner()}
    {
        trace_copy(r.m_owner);
    }

    shared_ptr(shared_ptr&& r) noexcept
        : m_ptr{std::move(r.m_ptr)}
        , m_logging{r.m_logging}
        , m_owner{create_owner()}
    {
        trace_move(r.m_owner);
    }

    template <class Y>
    shared_ptr(shared_ptr<Y>&& r) noexcept
        : m_ptr{std::move(r.m_ptr)}
        , m_logging{r.m_logging}
        , m_owner{create_owner()}
    {
        trace_move(r.m_owner);
    }

    template <class Y>
    explicit shared_ptr(const std::weak_ptr<Y>& r, flag_type log = {})
        : m_ptr{r}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    template<class Y, class Deleter>
    shared_ptr(std::unique_ptr<Y, Deleter>&& r, flag_type log = {})
        : m_ptr{std::move(r)}
        , m_logging{log}
        , m_owner{create_owner()}
    {
        trace_create();
    }

    ~shared_ptr()
    {
        if(m_ptr) {
            trace_clear();
        }
    }

    template <typename Owner = owner_type>
    requires (std::is_same_v<Owner, pe::Owner>)
    void take_ownership(Owner& to, const Owner& from)
    {
        to.m_raw = from.m_raw;
        to.m_ownership_id = from.m_ownership_id;
        to.m_typename = from.m_typename;
    }

    template <typename Owner = owner_type>
    requires (!std::is_same_v<Owner, pe::Owner>)
    void take_ownership(Owner& to, const Owner& from)
    {}

    template <typename Owner = owner_type, typename Y>
    requires (std::is_same_v<Owner, pe::Owner>)
    void reset_ownership(Owner& owner, std::shared_ptr<Y>& ptr)
    {
        owner.m_ownership_id = ownership_id(ptr);
        owner.m_raw = ptr;
        owner.m_typename = typeid(Y).name();
    }

    template <typename Owner = owner_type, typename Y>
    requires (!std::is_same_v<Owner, pe::Owner>)
    void reset_ownership(Owner& owner, std::shared_ptr<Y>& ptr)
    {}

    template <typename Owner = owner_type>
    requires (std::is_same_v<Owner, pe::Owner>)
    void swap_ownership(Owner& first, Owner& second)
    {
        std::swap(first.m_ownership_id, second.m_ownership_id);
        std::swap(first.m_raw, second.m_raw);
        std::swap(first.m_typename, second.m_typename);
    }

    template <typename Owner = owner_type>
    requires (!std::is_same_v<Owner, pe::Owner>)
    void swap_ownership(Owner& first, Owner& second)
    {}

    shared_ptr& operator=(const shared_ptr& r) noexcept
    {
        m_ptr = r.m_ptr;
        m_logging = r.m_logging;
        take_ownership(m_owner, r.m_owner);
        trace_copy(r.m_owner);
        return *this;
    }

    template <class Y>
    shared_ptr& operator=(const shared_ptr<Y>& r) noexcept
    {
        m_ptr = r.m_ptr;
        m_logging = r.m_logging;
        take_ownership(m_owner, r.m_owner);

        trace_copy(r.m_owner);
        return *this;
    }

    shared_ptr& operator=(shared_ptr&& r) noexcept
    {
        m_ptr = std::move(r.m_ptr);
        m_logging = r.m_logging;
        take_ownership(m_owner, r.m_owner);

        trace_move(r.m_owner);
        return *this;
    }

    template <class Y>
    shared_ptr& operator=(shared_ptr<Y>&& r) noexcept
    {
        m_ptr = std::move(r.m_ptr);
        m_logging = r.m_logging;
        take_ownership(m_owner, r.m_owner);

        trace_move(r.m_owner);
        return *this;
    }

    template <class Y, class Deleter>
    shared_ptr& operator=(std::unique_ptr<Y,Deleter>&& r)
    {
        m_ptr = std::move(r);
        take_ownership(m_owner, r.m_owner);

        trace_create();
        return *this;
    }

    void reset() noexcept
    {
        if(m_ptr) {
            trace_clear();
        }
        m_ptr.reset();
        reset_ownership(m_owner, m_ptr);
    }

    template <class Y>
    void reset(Y *ptr)
    {
        if(m_ptr) {
            trace_clear();
        }

        m_ptr.reset(ptr);
        reset_ownership(m_owner, m_ptr);

        trace_create();
    }

    template <class Y, class Deleter>
    void reset(Y *ptr, Deleter d)
    {
        if(m_ptr) {
            trace_clear();
        }

        m_ptr.reset(ptr, d);
        reset_ownership(m_owner, m_ptr);

        trace_create();
    }

    template <class Y, class Deleter, class Alloc>
    void reset(Y *ptr, Deleter d, Alloc alloc)
    {
        if(m_ptr) {
            trace_clear();
        }

        m_ptr.reset(ptr, d, alloc);
        reset_ownership(m_owner, m_ptr);

        trace_create();
    }

    void swap(shared_ptr& r) noexcept
    {
        trace_move(r.m_owner);

        std::swap(m_ptr, r.m_ptr);
        swap_ownership(m_owner, r.m_owner);

        trace_move(m_owner);
    }

    element_type *get() const noexcept
    {
        return m_ptr.get();
    }

    template <typename U = T>
    requires (!std::is_void_v<U>)
    U& operator*() const noexcept
    {
        return m_ptr.operator*();
    }

    template <typename U = T>
    requires (!std::is_void_v<U>)
    U* operator->() const noexcept
    {
        return m_ptr.operator->();
    }

    template <typename U = element_type>
    requires (!std::is_void_v<U>)
    U& operator[](std::ptrdiff_t idx) const
    {
        return m_ptr.operator[](idx);
    }

    long use_count() const noexcept
    {
        return m_ptr.use_count();
    }

    explicit operator bool() const noexcept
    {
        return m_ptr.operator bool();
    }

    template <class Y>
    bool owner_before(const shared_ptr<Y>& other) const noexcept
    {
        m_ptr.owner_before(other);
    }

    template <class Y>
    bool owner_before(const std::weak_ptr<Y>& other) const noexcept
    {
        m_ptr.owner_before(other);
    }

    template <class U>
    bool operator==(const pe::shared_ptr<U>& rhs) const noexcept
    {
        return (m_ptr == rhs.m_ptr);
    }

    bool operator==(const std::nullptr_t rhs) const noexcept
    {
        return (m_ptr == rhs);
    }

    template <class U>
    std::strong_ordering operator<=>(const shared_ptr<U>& rhs) const noexcept
    {
        return (m_ptr <=> rhs.m_ptr);
    }

    std::strong_ordering operator<=>(std::nullptr_t rhs) noexcept
    {
        return (m_ptr <=> rhs);
    }

    template <typename Y>
    friend class shared_ptr;

    template <class Y>
    friend class enable_shared_from_this;

    template <class Y, bool Log, class... Args>
    friend shared_ptr<Y> make_shared(Args&&... args);

    template <class Y, bool Log>
    friend shared_ptr<Y> make_shared(std::size_t N);

    template <class Y, bool Log>
    friend shared_ptr<Y> make_shared();

    template <class Y, bool Log>
    friend shared_ptr<Y> make_shared(std::size_t N, const std::remove_extent_t<Y>& u);

    template <class Y, bool Log>
    friend shared_ptr<Y> make_shared(const std::remove_extent_t<Y>& u);

    template <class Y, class Alloc, bool Log, class... Args>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc, Args&&... args);

    template <class Y, class Alloc, bool Log>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc, std::size_t N);

    template <class Y, class Alloc, bool Log>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc);

    template <class Y, class Alloc, bool Log>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc, std::size_t N,
                                         const std::remove_extent_t<Y>& u);
    template <class Y, class Alloc, bool Log>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc,
                                         const std::remove_extent_t<Y>& u);

    template <class Y, class U>
    friend shared_ptr<Y> static_pointer_cast(const shared_ptr<U>& r) noexcept;

    template <class Y, class U>
    friend shared_ptr<Y> static_pointer_cast(shared_ptr<U>&& r) noexcept;

    template <class Y, class U>
    friend shared_ptr<Y> dynamic_pointer_cast(const shared_ptr<U>& r) noexcept;

    template <class Y, class U>
    friend shared_ptr<Y> dynamic_pointer_cast(shared_ptr<U>&& r) noexcept;

    template <class Y, class U >
    friend shared_ptr<Y> const_pointer_cast(const shared_ptr<U>& r) noexcept;

    template <class Y, class U>
    friend shared_ptr<Y> const_pointer_cast(shared_ptr<U>&& r) noexcept;

    template <class Y, class U>
    friend shared_ptr<T> reinterpret_pointer_cast(const shared_ptr<U>& r) noexcept;

    template <class Y, class U>
    friend shared_ptr<Y> reinterpret_pointer_cast(shared_ptr<U>&& r) noexcept;

    template <class Deleter, class Y>
    friend Deleter* get_deleter(const shared_ptr<Y>& p) noexcept;

    template <class Y, class U, class V>
    friend std::basic_ostream<U, V>& ::operator<<(std::basic_ostream<U, V>& os, 
                                                  const shared_ptr<Y>& ptr);
};

template <typename T>
void check()
{
    static_assert(!kDebug ? sizeof(shared_ptr<T>) == sizeof(std::shared_ptr<T>) : true);
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
template <class T, bool Log = false, class... Args>
shared_ptr<T> make_shared(Args&&... args)
{
    shared_ptr<T> ret{
        std::move(std::make_shared<T, Args...>(std::forward<Args>(args)...)),
        flag_arg_v<Log>
    };
    return ret;
}

export
template <class T, bool Log = false>
shared_ptr<T> make_shared(std::size_t N)
{
    shared_ptr<T> ret{std::move(std::make_shared<T>(N)), flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Log = false>
shared_ptr<T> make_shared()
{
    shared_ptr<T> ret{std::move(std::make_shared<T>()), flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Log = false>
shared_ptr<T> make_shared(std::size_t N, const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret{std::move(std::make_shared<T>(N, u)), flag_arg_v<Log>};
    return ret;
}

export
template <class T, bool Log = false>
shared_ptr<T> make_shared(const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret{std::move(std::make_shared<T>(u)), flag_arg_v<Log>};
    return ret;
}

template <class T, class Alloc, bool Log = false, class... Args>
shared_ptr<T> allocate_shared(const Alloc& alloc, Args&&... args)
{
    shared_ptr<T> ret{
        std::move(std::allocate_shared<T, Alloc, Args...>(alloc, std::forward<Args>(args)...)),
        flag_arg_v<Log>
    };
    return ret;
}

template <class T, class Alloc, bool Log = false>
shared_ptr<T> allocate_shared(const Alloc& alloc, std::size_t N)
{
    shared_ptr<T> ret{
        std::move(std::allocate_shared<T, Alloc>(alloc, N)),
        flag_arg_v<Log>
    };
    return ret;
}

template <class T, class Alloc, bool Log = false>
shared_ptr<T> allocate_shared(const Alloc& alloc)
{
    shared_ptr<T> ret{std::move(std::allocate_shared<T, Alloc>(alloc)), flag_arg_v<Log>};
    return ret;
}

template <class T, class Alloc, bool Log = false>
shared_ptr<T> allocate_shared(const Alloc& alloc, std::size_t N,
                              const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret{
        std::move(std::allocate_shared<T, Alloc>(alloc, N, u)),
        flag_arg_v<Log>
    };
    return ret;
}

template <class T, class Alloc, bool Log = false>
shared_ptr<T> allocate_shared(const Alloc& alloc,
                              const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret{
        std::move(std::allocate_shared<T, Alloc>(alloc, u)),
        flag_arg_v<Log>
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> static_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::static_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> static_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::static_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> dynamic_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::dynamic_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> dynamic_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::dynamic_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> const_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::const_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> const_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::const_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> reinterpret_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::reinterpret_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class T, class U>
shared_ptr<T> reinterpret_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret{
        std::move(std::reinterpret_pointer_cast<T, U>(r.m_ptr)),
        r.m_logging
    };
    return ret;
}

export
template <class Deleter, class T>
Deleter* get_deleter(const shared_ptr<T>& p) noexcept
{
    return std::get_deleter<Deleter, T>(p.m_ptr);
}

export
template <class T>
class enable_shared_from_this : public std::enable_shared_from_this<T>
{
public:

    constexpr enable_shared_from_this() noexcept
        : std::enable_shared_from_this<T>()
    {}

    enable_shared_from_this(const enable_shared_from_this<T>& obj) noexcept
        : std::enable_shared_from_this<T>(obj)
    {}

    ~enable_shared_from_this() = default;

    enable_shared_from_this<T>& operator=(const enable_shared_from_this<T>& obj) noexcept
    {
        return std::enable_shared_from_this<T>::operator=(obj);
    }

    template <bool Log = false>
    shared_ptr<T> shared_from_this()
    {
        shared_ptr<T> ret{
            std::move(std::enable_shared_from_this<T>::shared_from_this()),
            flag_arg_v<Log>
        };
        return ret;
    }

    template <bool Log = false>
    shared_ptr<T const> shared_from_this() const
    {
        shared_ptr<T> ret{
            std::move(std::enable_shared_from_this<T const>::shared_from_this()),
            flag_arg_v<Log>
        };
        return ret;
    }
};

template <typename T>
class weak_ptr
{
private:

    std::weak_ptr<T> m_ptr;

public:

    using element_type = typename std::weak_ptr<T>::element_type;

    constexpr weak_ptr() noexcept
        : m_ptr{}
    {}

    weak_ptr(const weak_ptr& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    template <class Y>
    weak_ptr(const weak_ptr<Y>& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    template <class Y>
    weak_ptr(const shared_ptr<Y>& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    weak_ptr(weak_ptr&& r) noexcept
        : m_ptr{std::move(r.m_ptr)}
    {}

    template <class Y>
    weak_ptr(weak_ptr<Y>&& r) noexcept
        : m_ptr{std::move(r.m_ptr)}
    {}

    ~weak_ptr() = default;

    weak_ptr& operator=(const weak_ptr& r) noexcept
    {
        m_ptr = r.m_ptr;
        return *this;
    }

    template<class Y>
    weak_ptr& operator=(const weak_ptr<Y>& r) noexcept
    {
        m_ptr = r.m_ptr;
        return *this;
    }

    template<class Y>
    weak_ptr& operator=(const shared_ptr<Y>& r) noexcept
    {
        m_ptr = r.m_ptr;
        return *this;
    }

    weak_ptr& operator=(weak_ptr&& r) noexcept
    {
        m_ptr = std::move(r.m_ptr);
        return *this;
    }

    template<class Y>
    weak_ptr& operator=(weak_ptr<Y>&& r) noexcept
    {
        m_ptr = std::move(r.m_ptr);
        return *this;
    }

    void reset() noexcept
    {
        m_ptr.reset();
    }

    void swap(weak_ptr& r) noexcept
    {
        m_ptr.swap(r.m_ptr);
    }

    long use_count() const noexcept
    {
        return m_ptr.use_count();
    }

    bool expired() const noexcept
    {
        return m_ptr.expired();
    }

    shared_ptr<T> lock() const noexcept
    {
        return shared_ptr<T>{std::move(m_ptr.lock())};
    }
};

template <class T>
weak_ptr(shared_ptr<T>) -> weak_ptr<T>;

}; // namespace pe

template <class T, class U, class V>
std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os, 
                                     const pe::shared_ptr<T>& ptr)
{
    return operator<<(os, ptr.m_ptr);
}

