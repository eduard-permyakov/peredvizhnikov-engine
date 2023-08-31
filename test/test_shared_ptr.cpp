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
import <future>;
import <vector>;

constexpr int kNumPointersProduced = 1000;

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
    pe::dbgprint("pe::weak_ptr testing completed.");
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

void test_atomic_shared_ptr()
{
    struct test
    {
        int x;
        int y;
        ~test()
        {
            pe::dbgprint("test instance deleted!");
        }
    };

    pe::shared_ptr ptr = pe::make_shared<test, true, true>(99, 69);
    pe::atomic_shared_ptr atomic{ptr};
    ptr.reset();

    pe::dbgprint("atomic_shared_ptr now holds exclusive ownership.");

    ptr = atomic.load(std::memory_order_relaxed);
    pe::assert(ptr->x == 99);
    pe::assert(ptr->y == 69);

    /* Make sure we can store and retrieve an aliasing shared_ptr */
    pe::shared_ptr<int> aliasing{ptr, &ptr->y};
    ptr.reset();
    pe::atomic_shared_ptr atomic_aliasing{aliasing};

    aliasing = atomic_aliasing.load(std::memory_order_relaxed);
    pe::assert(*aliasing == 69);
    aliasing.reset();

    pe::dbgprint("Clearing atomic_shared_ptr.");
    auto old = atomic.exchange(pe::shared_ptr<test>{nullptr}, std::memory_order_relaxed);
    pe::assert(old->x == 99);
    pe::assert(old->y == 69);
    old.reset();

    atomic_aliasing.store(pe::shared_ptr<int>{nullptr}, std::memory_order_relaxed);
}

void pointer_producer(pe::atomic_shared_ptr<int>& ptr)
{
    int produced = 0;
    pe::shared_ptr<int> expected{nullptr};

    while(produced < kNumPointersProduced) {

        auto newptr = pe::make_shared<int>(produced);
        do{
            expected = pe::shared_ptr<int>{nullptr};
        }while(!ptr.compare_exchange_weak(expected, newptr,
            std::memory_order_release, std::memory_order_relaxed));
        produced++;
    }
}

void pointer_consumer(pe::atomic_shared_ptr<int>& ptr)
{
    int consumed = 0;
    pe::shared_ptr<int> null{nullptr};
    while(consumed < kNumPointersProduced) {

        pe::shared_ptr<int> expected{nullptr};
        while(!(expected = ptr.load(std::memory_order_acquire)));
        bool result = ptr.compare_exchange_weak(expected, null,
            std::memory_order_release, std::memory_order_relaxed);

        pe::assert(result);
        pe::assert(*expected == consumed);
        consumed++;
    }
}

void test_atomic_shared_ptr_advanced()
{
    pe::atomic_shared_ptr<int> ptr{};

    std::vector<std::future<void>> tasks{};
    tasks.push_back(std::async(std::launch::async, pointer_producer, std::ref(ptr)));
    tasks.push_back(std::async(std::launch::async, pointer_consumer, std::ref(ptr)));

    for(const auto& task : tasks) {
        task.wait();
    }
    pe::dbgprint("Successfully produced and consumed", kNumPointersProduced, 
        "atomic shared pointers.");
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

    pe::ioprint(pe::TextColor::eGreen, "Testing pe::atomic_shared_ptr");
    test_atomic_shared_ptr();
    test_atomic_shared_ptr_advanced();
}

