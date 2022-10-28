import logger;
import shared_ptr;
import platform;

import <iostream>;
import <memory>;
import <thread>;
import <chrono>;
import <mutex>;

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
    virtual ~Base() { pe::ioprint(pe::LogLevel::eWarning, "  Base::~Base()"); }
};
 
struct Derived: public Base
{
    Derived() { pe::ioprint(pe::LogLevel::eWarning, "  Derived::Derived()"); }
    virtual ~Derived() { pe::ioprint(pe::LogLevel::eWarning, "  Derived::~Derived()"); }
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
 
int main()
{
    pe::ioprint(pe::LogLevel::eNotice, "Testing std::shared_ptr:");
    std::shared_ptr<Base> std = std::make_shared<Derived>();
    test(std);

    pe::ioprint(pe::LogLevel::eNotice, "Testing pe::shared_ptr:");
    pe::shared_ptr<Base> pe = pe::make_shared<Derived>();
    pe.LogOwners();
    test(pe);
}

