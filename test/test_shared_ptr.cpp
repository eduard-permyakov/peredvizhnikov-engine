import logger;
import shared_ptr;
import platform;
import assert;

import <iostream>;
import <memory>;
import <thread>;
import <chrono>;
import <mutex>;
import <variant>;

template <typename Ptr, typename T>
concept SharedPtr = requires(Ptr ptr)
{
    {ptr.get()} -> std::same_as<std::remove_extent_t<T>*>;
    {ptr.use_count()} -> std::same_as<long>;
    requires (std::is_copy_assignable_v<Ptr>);
};
 
struct Base
{
    Base() { pe::ioprint(pe::LogLevel::eWarning, "  Base::Base()"); }
    /* virtual destructor not necessary if only using shared_ptr */
    ~Base() { pe::ioprint(pe::LogLevel::eWarning, "  Base::~Base()"); }
};
 
struct Derived: public Base
{
    Derived() { pe::ioprint(pe::LogLevel::eWarning, "  Derived::Derived()"); }
    ~Derived() { pe::ioprint(pe::LogLevel::eWarning, "  Derived::~Derived()"); }
};
 
template <SharedPtr<Base> PtrType>
void thr(PtrType p, int secs)
{
    std::this_thread::sleep_for(std::chrono::seconds(secs));
    /* thread-safe, even though shared use_count is incremented */
    PtrType lp = p;
    pe::dbgprint("local pointer in a thread:");
    pe::dbgprint("  lp.get() =", lp.get(), ", lp.use_count() =", lp.use_count());

    if constexpr (std::derived_from<PtrType, pe::shared_ptr<Base>>) {
        p.LogOwners();
    }
}

template <SharedPtr<Base> PtrType>
void test(PtrType& p)
{
    pe::dbgprint("Created a shared Derived (as a pointer to Base)");
    pe::dbgprint("  p.get() =", p.get(), ", p.use_count() =", p.use_count());

    std::thread t1{thr<PtrType>, p, 1}, t2{thr<PtrType>, p, 2}, t3{thr<PtrType>, p, 3};
    p.reset(); /* release ownership from main */

    pe::dbgprint("Shared ownership between 3 threads and released");
    pe::dbgprint("ownership from main:");
    pe::dbgprint("  p.get() =", p.get(), ", p.use_count() =", p.use_count());

    t1.join(); t2.join(); t3.join();
    pe::dbgprint("All threads completed, the last one deleted Derived");
}


void test_shared_ownership()
{
    struct object
    {
        int x, y;
    };

    pe::shared_ptr<object> p1 = pe::make_shared<object, true>();
    pe::shared_ptr<int>    p2{p1, &p1->y};

    /* These should both report 2 identical owners, despite 
     * not comparing equal. */
    p1.LogOwners();
    p2.LogOwners();

    object o;
    auto flag = pe::shared_ptr<object>::true_value();
    pe::shared_ptr<object> o1{&o, [](object*){ pe::dbgprint("Cleaning up o1"); }, flag};
    pe::shared_ptr<object> o2{&o, [](object*){ pe::dbgprint("Cleaning up o2"); }, flag};

    /* These should both having 1 distinct owner, despite
     * comparing equal. */
     o1.LogOwners();
     o2.LogOwners();
}

void test_weak_ptr()
{
    pe::shared_ptr<int> ptr = pe::make_shared<int, true>(5);
    pe::weak_ptr<int> weak{ptr};

    pe::assert(ptr.use_count() == 1);
    pe::assert(weak.use_count() == 1);

    pe::shared_ptr<int> copy = weak.lock();
    pe::assert(ptr.use_count() == 2);
    pe::assert(weak.use_count() == 2);

    ptr.LogOwners();
    copy.reset();
}

void test_from_unique()
{
    std::unique_ptr<int, void(*)(int*)> unique{new int, [](int *ptr){ 
        pe::dbgprint("Deleting int!");
        delete ptr; 
    }};
    pe::shared_ptr<int> shared{std::move(unique)};
    /* The shared ptr has taken ownership from the unique ptr */
    pe::assert(unique.get() == nullptr);
}

void test_incomlete_type()
{
    struct incomplete;
    pe::shared_ptr<incomplete> ptr{nullptr};

    struct incomplete
    {
        ~incomplete()
        {
            pe::dbgprint("Deleting 'incomplete' instance!");
        }
    };

    ptr = pe::make_shared<incomplete>();
}

void test_array()
{
    pe::shared_ptr<int[]> ptr = pe::make_shared<int[]>(64);
    ptr[12] = 69;
    pe::assert(ptr[12] == 69);
    /* our implementation will correctly call delete[] on the stored pointer */
    auto deleter = pe::get_deleter<std::default_delete<int[]>>(ptr);
    pe::assert(deleter);
}

void test_allocator()
{
    std::allocator<std::byte> alloc{};
    pe::shared_ptr<float> ptr = pe::allocate_shared<float, decltype(alloc)>(alloc, 12.0f);
    pe::assert(*ptr == 12.0f);
}
 
int main()
{
    pe::ioprint(pe::TextColor::eGreen, "Testing std::shared_ptr:");
    std::shared_ptr<Base> std = std::make_shared<Derived>();
    test(std);

    pe::ioprint(pe::TextColor::eGreen, "Testing pe::shared_ptr:");
    pe::shared_ptr<Base> pe = pe::make_shared<Derived, true, true>();
    pe.LogOwners();
    test(pe);

    pe::ioprint(pe::TextColor::eGreen, "Testing pe::weak_ptr:");
    test_weak_ptr();

    pe::ioprint(pe::TextColor::eGreen, "Testing pe::shared_ptr edge cases:");
    test_shared_ownership();
    test_from_unique();
    test_incomlete_type();
    test_array();
    test_allocator();
}

