export module shared_ptr;

import <memory>;

/*
 * Forward declarations
 */
namespace pe
{
    export template <typename T> class shared_ptr;
};

export
template <class T, class U, class V>
std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os, 
                                     const pe::shared_ptr<T>& ptr);

namespace pe{

/* 
 * A wrapper around std::shared_ptr with additional debugging and 
 * tracing facilities.
 */
template <typename T>
class shared_ptr
{
private:

    std::shared_ptr<T> m_ptr;

public:

    using element_type = typename std::shared_ptr<T>::element_type;
    using weak_type = typename std::shared_ptr<T>::weak_type;

    constexpr shared_ptr() noexcept
        : m_ptr{}
    {}

    constexpr shared_ptr(std::nullptr_t ptr) noexcept
        : m_ptr{ptr}
    {}

    template <class Y>
    explicit shared_ptr(Y *ptr)
        : m_ptr{ptr}
    {}

    template <class Y, class Deleter>
    shared_ptr(Y *ptr, Deleter d)
        : m_ptr{ptr, d}
    {}

    template<class Deleter>
    shared_ptr(std::nullptr_t ptr, Deleter d)
        : m_ptr{ptr, d}
    {}

    template <class Y, class Deleter, class Alloc>
    shared_ptr(Y *ptr, Deleter d, Alloc alloc)
        : m_ptr{ptr, d, alloc}
    {}

    template <class Deleter, class Alloc>
    shared_ptr(std::nullptr_t ptr, Deleter d, Alloc alloc)
        : m_ptr{ptr, d, alloc}
    {}

    template <class Y>
    shared_ptr(const shared_ptr<Y>& r, element_type* ptr) noexcept
        : m_ptr{r.m_ptr, ptr}
    {}

    template <class Y>
    shared_ptr(shared_ptr<Y>&& r, element_type* ptr) noexcept
        : m_ptr{r.m_ptr, ptr}
    {}

    shared_ptr(const shared_ptr& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    template <class Y>
    shared_ptr(const shared_ptr<Y>& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    shared_ptr(shared_ptr&& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    template <class Y>
    shared_ptr(shared_ptr<Y>&& r) noexcept
        : m_ptr{r.m_ptr}
    {}

    template <class Y>
    explicit shared_ptr(const std::weak_ptr<Y>& r)
        : m_ptr{r}
    {}

    template<class Y, class Deleter>
    shared_ptr(std::unique_ptr<Y, Deleter>&& r)
        : m_ptr{r}
    {}

    ~shared_ptr() = default;

    shared_ptr& operator=(const shared_ptr& r) noexcept
    {
        m_ptr.operator=(r.m_ptr);
        return *this;
    }

    template <class Y>
    shared_ptr& operator=(const shared_ptr<Y>& r) noexcept
    {
        m_ptr.operator=(r.m_ptr);
        return *this;
    }

    shared_ptr& operator=(shared_ptr&& r) noexcept
    {
        m_ptr.operator=(r.m_ptr);
        return *this;
    }

    template <class Y>
    shared_ptr& operator=(shared_ptr<Y>&& r) noexcept
    {
        m_ptr.operator=(r.m_ptr);
        return *this;
    }

    template <class Y, class Deleter>
    shared_ptr& operator=(std::unique_ptr<Y,Deleter>&& r)
    {
        m_ptr.operator=(r);
        return *this;
    }

    void reset() noexcept
    {
        m_ptr.reset();
    }

    template <class Y>
    void reset(Y *ptr)
    {
        m_ptr.reset(ptr);
    }

    template <class Y, class Deleter>
    void reset(Y *ptr, Deleter d)
    {
        m_ptr.reset(ptr, d);
    }

    template <class Y, class Deleter, class Alloc>
    void reset(Y *ptr, Deleter d, Alloc alloc)
    {
        m_ptr.reset(ptr, d, alloc);
    }

    void swap(shared_ptr& r) noexcept
    {
        m_ptr.swap(r);
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

    template <class Y, class... Args>
    friend shared_ptr<Y> make_shared(Args&&... args);

    template <class Y>
    friend shared_ptr<Y> make_shared(std::size_t N);

    template <class Y>
    friend shared_ptr<Y> make_shared();

    template <class Y>
    friend shared_ptr<Y> make_shared(std::size_t N, const std::remove_extent_t<Y>& u);

    template <class Y>
    friend shared_ptr<Y> make_shared(const std::remove_extent_t<Y>& u);

    template <class Y, class Alloc, class... Args>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc, Args&&... args);

    template <class Y, class Alloc>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc, std::size_t N);

    template <class Y, class Alloc>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc);

    template <class Y, class Alloc>
    friend shared_ptr<Y> allocate_shared(const Alloc& alloc, std::size_t N,
                                         const std::remove_extent_t<Y>& u);
    template <class Y, class Alloc>
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
struct shared_ptr_detail
{
    static_assert(sizeof(shared_ptr<T>) == sizeof(std::shared_ptr<T>));
};

export
template <class T, class... Args>
shared_ptr<T> make_shared(Args&&... args)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::make_shared<T, Args...>(std::forward<Args>(args)...));
    return ret;
}

export
template <class T>
shared_ptr<T> make_shared(std::size_t N)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::make_shared<T>(N));
    return ret;
}

export
template <class T>
shared_ptr<T> make_shared()
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::make_shared<T>());
    return ret;
}

export
template <class T>
shared_ptr<T> make_shared(std::size_t N, const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::make_shared<T>(N, u));
    return ret;
}

export
template <class T>
shared_ptr<T> make_shared(const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::make_shared<T>(u));
    return ret;
}

template <class T, class Alloc, class... Args>
shared_ptr<T> allocate_shared(const Alloc& alloc, Args&&... args)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::allocate_shared<T, Alloc, Args...>(alloc, std::forward<Args>(args)...));
    return ret;
}

template <class T, class Alloc>
shared_ptr<T> allocate_shared(const Alloc& alloc, std::size_t N)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::allocate_shared<T, Alloc>(alloc, N));
    return ret;
}

template <class T, class Alloc>
shared_ptr<T> allocate_shared(const Alloc& alloc)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::allocate_shared<T, Alloc>(alloc));
    return ret;
}

template <class T, class Alloc>
shared_ptr<T> allocate_shared(const Alloc& alloc, std::size_t N,
                              const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::allocate_shared<T, Alloc>(alloc, N, u));
    return ret;
}

template <class T, class Alloc>
shared_ptr<T> allocate_shared(const Alloc& alloc,
                              const std::remove_extent_t<T>& u)
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::allocate_shared<T, Alloc>(alloc, u));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> static_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::static_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> static_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::static_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> dynamic_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::dynamic_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> dynamic_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::dynamic_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> const_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::const_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> const_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::const_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> reinterpret_pointer_cast(const shared_ptr<U>& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::reinterpret_pointer_cast<T, U>(r.m_ptr));
    return ret;
}

export
template <class T, class U>
shared_ptr<T> reinterpret_pointer_cast(shared_ptr<U>&& r) noexcept
{
    shared_ptr<T> ret;
    ret.m_ptr = std::move(std::reinterpret_pointer_cast<T, U>(r.m_ptr));
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
        std::enable_shared_from_this<T>::operator=(obj);
    }

    shared_ptr<T> shared_from_this()
    {
        shared_ptr<T> ret;
        ret.m_ptr = std::move(std::enable_shared_from_this<T>::shared_from_this());
        return ret;
    }

    shared_ptr<T const> shared_from_this() const
    {
        shared_ptr<T> ret;
        ret.m_ptr = std::move(std::enable_shared_from_this<T>::shared_from_this());
        return ret;
    }
};

}; // namespace pe

template <class T, class U, class V>
std::basic_ostream<U, V>& operator<<(std::basic_ostream<U, V>& os, 
                                     const pe::shared_ptr<T>& ptr)
{
    return operator<<(os, ptr.m_ptr);
}

