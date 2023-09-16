export module taskgraph;

import sync;
import shared_ptr;
import meta;

import <vector>;
import <tuple>;
import <utility>;

namespace pe{

/*****************************************************************************/
/* BASE CLASS REGISTRY                                                       */
/*****************************************************************************/
/*
 * A CRTP base that allows detecting all registered base classes of a
 * particular derived type. We add a friend function to each base that needs 
 * to be detectable in this manner, and merely considering it during overload
 * resolution instantiates the template that appends the corresponding base
 * class to a compile-time list.
 */

template <typename T>
struct tag
{
    using type = T;
};

namespace detail
{
    constexpr void adl_view_base() {} /* A dummy ADL target. */

    template <typename D, std::size_t I>
    struct BaseViewer
    {
        friend constexpr auto adl_view_base(BaseViewer);
    };

    template <typename D, std::size_t I, typename B>
    struct BaseWriter
    {
        friend constexpr auto adl_view_base(BaseViewer<D, I>) 
        {
            return tag<B>{};
        }
    };

    template <typename D, typename Unique, std::size_t I = 0, typename = void>
    struct NumBases : std::integral_constant<std::size_t, I>
    {};

    template <typename D, typename Unique, std::size_t I>
    struct NumBases<D, Unique, I, decltype(adl_view_base(BaseViewer<D, I>{}), void())> 
        : std::integral_constant<std::size_t, NumBases<D, Unique, I+1, void>::value>
    {};

    template <typename D, typename B>
    struct BaseInserter : BaseWriter<D, NumBases<D, B>::value, B>
    {};

    template <typename T>
    constexpr void adl_register_bases(void*) {} /* A dummy ADL target. */

    template <typename T>
    struct RegisterBases 
        : decltype(adl_register_bases<T>((T *)nullptr), tag<void>{})
    {};

    template <typename T, typename I>
    struct BaseListLow{};

    template <typename T, std::size_t... I>
    struct BaseListLow<T, std::index_sequence<I...>>
    {
        static constexpr std::tuple<decltype(adl_view_base(BaseViewer<T, I>{}))...> helper() {}
        using type = decltype(helper());
    };

    template <typename T>
    struct BaseList : BaseListLow<T, 
        std::make_index_sequence<((void)detail::RegisterBases<T>(), NumBases<T, void>::value)>>
    {};
}

template <typename T>
using base_list_t = typename detail::BaseList<T>::type;

template <typename T>
struct Base
{
    template <
        typename D,
        std::enable_if_t<std::is_base_of_v<T, D>, std::nullptr_t> = nullptr,
        typename detail::BaseInserter<D, T>::non_existent = nullptr
    >
    friend constexpr void adl_register_bases(void *) {}
};

/*****************************************************************************/
/* READS/WRITES                                                              */
/*****************************************************************************/
/*
 * Mixin bases to encode dependencies between tasks as types.
 */

export 
template <typename T, typename Derived>
struct Reads : Base<Reads<T, Derived>>
{
    using type = T;
};

export
template <typename T, typename Derived>
struct Writes : Base<Writes<T, Derived>>
{
    using type = T;
};

/*****************************************************************************/
/* PHASE COMPLETION                                                          */
/*****************************************************************************/
/*
 * Yield value for a phase completion of a system.
 */
struct Token{};

export struct PhaseCompletion
{
private:
    template <typename ReturnType, typename TaskType>
    friend struct TaskPromise;
    PhaseCompletion() {}
public:
    constexpr PhaseCompletion(Token token) {}
};

export constexpr PhaseCompletion PhaseCompleted = PhaseCompletion{Token{}};

/*****************************************************************************/
/* TASK NODE                                                                 */
/*****************************************************************************/

class TaskNode : public Task<void, TaskNode, Barrier&, Barrier&>
{
    using base = Task<void, TaskNode, Barrier&, Barrier&>;
    using base::base;

    pe::shared_ptr<TaskBase> m_task;

    virtual TaskNode::handle_type Run(Barrier& start, Barrier& finish)
    {
        while(true);
    }

public:

    template <std::derived_from<TaskBase> Task>
    TaskNode(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity,
        pe::shared_ptr<Task> task)
        : base{token, scheduler, priority, mode, affinity}
        , m_task{task}
    {}
};

/*****************************************************************************/
/* TASK CREATE INFO                                                          */
/*****************************************************************************/

export
template <std::derived_from<TaskBase> Task, typename... Args>
struct TaskCreateInfo
{
private:

    Priority            m_priority;
    Affinity            m_affinity;
    std::tuple<Args...> m_args;


public:

    using task_type = Task;

    TaskCreateInfo(std::in_place_type_t<Task> task, Priority priority, 
        Affinity affinity, Args&&... args)
        : m_priority{priority}
        , m_affinity{affinity}
        , m_args{std::forward_as_tuple<Args>(args)...}
    {}
};

export
template <std::derived_from<TaskBase> Task, typename... Args>
auto make_task_create_info(Priority priority, Affinity affinity, Args&&... args)
{
    return TaskCreateInfo<Task, Args...>{
        std::in_place_type_t<Task>{}, 
        priority, affinity,
        std::forward<Args>(args)...
    }; 
}

/*****************************************************************************/
/* COMPILE-TIME DIRECTED ACYCLIC GRAPH                                       */
/*****************************************************************************/

template <typename From, typename To>
struct Edge
{
    using from = From;
    using to = To;
};

template <typename T>
concept CEdge = pe::is_template_instance_v<T, Edge>;

template <typename T>
concept CNode = std::derived_from<T, TaskBase>;

template <typename T>
concept CTuple = pe::is_template_instance_v<T, std::tuple>;

template <CNode A, CNode B>
struct Connected
{
    using a_bases = base_list_t<A>;
    using b_bases = base_list_t<B>;
    using a_writes = decltype(extract_matching(std::declval<a_bases>(), []<typename T>() constexpr{
        return is_template_instance_v<typename std::remove_cvref_t<T>::type, Writes>;
    }));
    using b_reads = decltype(extract_matching(std::declval<b_bases>(), []<typename T>() constexpr{
        return is_template_instance_v<typename std::remove_cvref_t<T>::type, Reads>;
    }));
    using a_outputs = decltype(transform_tuple(std::declval<a_writes>(), []<typename T>() constexpr{
        return std::declval<typename std::remove_cvref_t<T>::type::type>();
    }));
    using b_inputs = decltype(transform_tuple(std::declval<b_reads>(), []<typename T>() constexpr{
        return std::declval<typename std::remove_cvref_t<T>::type::type>();
    }));
    using common = decltype(extract_common(std::declval<a_outputs>(), std::declval<b_inputs>()));
    static constexpr bool value = (std::tuple_size_v<common> > 0);
};

template <CNode... Nodes>
struct EdgeList
{
    /****************************************/
    /* edges_for_node                       */
    /****************************************/

    template <typename, typename, typename...>
    struct edges_for_node;

    template <typename Node, typename Other>
    struct edges_for_node<Node, Other>
    {
        static constexpr auto edge()
        {
            if constexpr (std::is_same_v<Node, Other>) {
                static_assert(!Connected<Other, Node>::value, "Self-referencing node!");
                return std::tuple<>{};
            }else if constexpr (Connected<Node, Other>::value) {
                static_assert(!Connected<Other, Node>::value, "Two nodes depend on each other!");
                return std::declval<std::tuple<Edge<Node, Other>>>();
            }else{
                return std::tuple<>{};
            }
        }

        using result = decltype(edge());
    };

    template <typename Node, typename Other, typename... Rest>
    struct edges_for_node
    {
        static constexpr auto edges()
        {
            return std::tuple_cat(
                std::declval<typename edges_for_node<Node, Other>::result>(),
                std::declval<typename edges_for_node<Node, Rest...>::result>()
            );
        }

        using result = decltype(edges());
    };

    /****************************************/
    /* compute_edges                        */
    /****************************************/

    template <typename...>
    struct compute_edges;

    template <>
    struct compute_edges<>
    {
        using result = std::tuple<>;
    };

    template <typename Head, typename... Tail>
    struct compute_edges<Head, Tail...>
    {
        using result = decltype(std::tuple_cat(
            std::declval<typename edges_for_node<Head, Nodes...>::result>(), 
            std::declval<typename compute_edges<Tail...>::result>()));
    };

    /****************************************/
    /* edges                                */
    /****************************************/

    using edges = typename compute_edges<Nodes...>::result;
};

template <CNode... Tasks>
struct NodeSet
{
    using elements = std::tuple<Tasks...>;

    template <typename... FArgs>
    static constexpr decltype(auto) create(std::tuple<FArgs...>&&)
    {
        return std::declval<NodeSet<FArgs...>>();
    }

    template <CNode Task>
    constexpr static bool Contains()
    {
        return contains_type_v<Task, elements>;
    }

    template <CNode Node>
    constexpr static decltype(auto) AfterInsert()
    {
        using new_elements = decltype(std::tuple_cat(
            std::declval<std::tuple<Node>>(),
            std::declval<elements>()));
        return std::declval<decltype(create(std::declval<new_elements>()))>();
    }
};

template <typename T>
concept CNodeSet = pe::is_template_instance_v<T, NodeSet>;

template <CNode... Nodes>
struct NodeStack
{
    using elements = std::tuple<Nodes...>;

    template <typename... FArgs>
    static constexpr decltype(auto) create(std::tuple<FArgs...>&&)
    {
        return std::declval<NodeStack<std::remove_cvref_t<FArgs>...>>();
    }

    template <typename Tuple, std::size_t... Is>
    static constexpr decltype(auto) pop_front_helper(Tuple&&, std::index_sequence<Is...>)
    {
        return std::declval<std::tuple<std::tuple_element_t<1 + Is, Tuple>...>>();
    }

    template <typename Tuple>
    static constexpr decltype(auto) pop_front(Tuple&&)
    {
        constexpr std::size_t size = std::tuple_size_v<Tuple>;
        static_assert(size > 0);
        if constexpr (size == 1) {
            return std::declval<std::tuple<>>();
        }else{
            return pop_front_helper(std::declval<Tuple>(), 
                std::make_index_sequence<std::tuple_size_v<Tuple> - 1>());
        }
    }

    constexpr static decltype(auto) AfterPop()
    {
        using new_elements = std::remove_reference_t<decltype(pop_front(std::declval<elements>()))>;
        return std::declval<decltype(create(std::declval<new_elements>()))>();
    }

    template <CNode Node>
    constexpr static decltype(auto) AfterPush()
    {
        using new_elements = decltype(std::tuple_cat(
            std::declval<std::tuple<Node>>(),
            std::declval<elements>()));
        return std::declval<decltype(create(std::declval<new_elements>()))>();
    }

    template <CNode... Args>
    constexpr static decltype(auto) AfterPushAll(std::tuple<Args...>&&)
    {
        using new_elements = decltype(std::tuple_cat(
            std::declval<std::tuple<Args...>>(),
            std::declval<elements>()));
        return std::declval<decltype(create(std::declval<new_elements>()))>();
    }

    template <std::size_t N>
    constexpr static decltype(auto) AfterPop()
    {
        static_assert(Size() >= N);
        constexpr std::size_t left = Size() - N;
        using new_elements = decltype(extract_tuple(make_seq<left, N>{}, std::declval<elements>()));
        return std::declval<decltype(create(std::declval<new_elements>()))>();
    }

    constexpr static std::size_t Size()
    {
        return std::tuple_size_v<elements>;
    }

    constexpr static decltype(auto) Top()
    {
        return std::declval<std::tuple_element_t<0, elements>>();
    }

    template <CNode Task>
    constexpr static bool Contains()
    {
        return contains_type_v<Task, elements>;
    }
};

template <typename T>
concept CNodeStack = pe::is_template_instance_v<T, NodeStack>;

template <typename F>
concept CVisitor = requires (F f){
    {f.template operator()<TaskBase>()};
};

template <CNode... Tasks>
struct DAG
{
    using nodes = std::tuple<Tasks...>;
    using edges = typename EdgeList<Tasks...>::edges;

    struct Empty{};
    struct CycleDetected{};

    /****************************************/
    /* for_all                              */
    /****************************************/

    template <template <typename> typename F, CTuple Args, typename...>
    struct for_all;

    template <template <typename> typename F, CTuple Args>
    struct for_all<F, Args>
    {
        using result = std::tuple<>;
    };

    template <template <typename> typename F, CTuple Args, typename Head, typename... Tail>
    struct for_all<F, Args, Head, Tail...>
    {
        template <typename... FArgs>
        static constexpr decltype(auto) call(std::tuple<FArgs...>&&)
        {
            return F<Head>{}.template operator()<FArgs...>();
        }

        using result = decltype(std::tuple_cat(
            call(std::declval<Args>()),
            std::declval<typename for_all<F, Args, Tail...>::result>()));
    };

    template <template <typename> typename F, CTuple Args, CTuple Tuple>
    struct for_all<F, Args, Tuple>
    {
        template <typename... TArgs>
        static constexpr decltype(auto) unpack(std::tuple<TArgs...>&&)
        {
            return std::declval<
                typename for_all<F, Args, TArgs...>::result
            >();
        }

        using result = decltype(unpack(std::declval<Tuple>()));
    };

    /****************************************/
    /* child_for_edge                       */
    /****************************************/

    template <CNode Node>
    struct child_for_edge
    {
        template <CEdge Edge>
        struct callable
        {
            template <typename... Args>
            constexpr decltype(auto) operator()()
            {
                if constexpr (std::is_same_v<Node, typename Edge::from>) {
                    return std::declval<std::tuple<typename Edge::to>>();
                }else{
                    return std::tuple<>{};
                }
            }
        };
    };

    /****************************************/
    /* input_for_node                       */
    /****************************************/

    template <CNode Node>
    struct input_for_node
    {
        template <typename... Args>
        constexpr decltype(auto) operator()()
        {
            using to = decltype(extract_matching(std::declval<edges>(), []<typename T>() constexpr{
                return std::is_same_v<typename std::remove_cvref_t<T>::to, Node>;
            }));
            if constexpr (std::tuple_size_v<to> == 0) {
                return std::declval<std::tuple<Node>>();
            }else{
                return std::tuple<>{};
            }
        }
    };

    /****************************************/
    /* output_for_node                      */
    /****************************************/

    template <CNode Node>
    struct output_for_node
    {
        template <typename... Args>
        constexpr decltype(auto) operator()()
        {
            using from = decltype(extract_matching(std::declval<edges>(), []<typename T>() constexpr{
                return std::is_same_v<typename std::remove_cvref_t<T>::from, Node>;
            }));
            if constexpr (std::tuple_size_v<from> == 0) {
                return std::declval<std::tuple<Node>>();
            }else{
                return std::tuple<>{};
            }
        }
    };

    /****************************************/
    /* dfs_helper                           */
    /****************************************/

    /* GrayNodes hold all the nodes whose subtrees are being 
     * visited in the current context. (i.e. the chain of nodes
     * from the root to Head).
     */
    template <CNodeSet Set, CNodeStack Stack, CNodeStack GrayNodes, std::size_t RDepth, 
        CVisitor Visitor, CNode Head>
    constexpr static auto dfs_root()
    {
        if constexpr (GrayNodes::template Contains<Head>()) {
            using ret = std::pair<std::tuple<CycleDetected>, Set>;
            return std::declval<ret>();
        }else if constexpr (Set::template Contains<Head>()) {
            using ret = std::pair<std::tuple<>, Set>;
            return std::declval<ret>();
        }else{
            using popped_stack = std::remove_reference_t<decltype(Stack::AfterPop())>;
            using result = decltype(Visitor{}.template operator()<Head>());
            using children = std::remove_reference_t<decltype(Children<Head>())>;

            using next_set = std::remove_reference_t<
                decltype(Set::template AfterInsert<Head>())>;
            using next_stack = std::remove_reference_t<
                decltype(popped_stack::template AfterPushAll(std::declval<children>()))>;

            constexpr bool has_children = (std::tuple_size_v<children> > 0);
            constexpr std::size_t next_depth = (has_children) ? RDepth + 1 : 0;

            if constexpr (next_stack::Size() == 0) {
                using ret = std::pair<std::tuple<result>, Set>;
                return std::declval<ret>();
            }else{
                using next = std::remove_reference_t<decltype(next_stack::Top())>;
                using pushed_gray_nodes = std::remove_reference_t<
                    decltype(GrayNodes::template AfterPush<Head>())>;
                using next_gray_nodes = std::conditional_t<
                    has_children,
                    pushed_gray_nodes,
                    std::remove_reference_t<decltype(pushed_gray_nodes::template AfterPop<RDepth>())>
                >;
                using child_retval = decltype(dfs_root<next_set, next_stack, 
                    next_gray_nodes, next_depth, Visitor, next>());
                using all_results = decltype(std::tuple_cat(std::declval<std::tuple<result>>(),
                    std::declval<typename child_retval::first_type>()));

                using ret = std::pair<all_results, typename child_retval::second_type>;
                return std::declval<ret>();
            }
        }
    }

    template <CNodeSet Set, CVisitor Visitor>
    constexpr static auto dfs_helper()
    {
        return std::tuple<>{};
    }

    template <CNodeSet Set, CVisitor Visitor, CNode Head, CNode... Tail>
    constexpr static auto dfs_helper()
    {
        if constexpr (Set::template Contains<Head>()) {
            return std::tuple<>{};
        }else{
            using stack = NodeStack<Head>;
            using result = decltype(dfs_root<Set, stack, NodeStack<>, 0, Visitor, Head>());
            using next_set = typename result::second_type;

            using ret = decltype(std::tuple_cat(
                std::declval<typename result::first_type>(),
                std::declval<decltype(dfs_helper<next_set, Visitor, Tail...>())>()
            ));
            return std::declval<ret>();
        }
    }

    /****************************************/
    /* Top-level API                        */
    /****************************************/

    constexpr static decltype(auto) Inputs()
    {
        using inputs = typename for_all<
            input_for_node,
            std::tuple<int, double>,
            nodes
        >::result;
        return std::declval<inputs>();
    }

    constexpr static decltype(auto) Outputs()
    {
        using outputs = typename for_all<
            output_for_node,
            std::tuple<int, double>,
            nodes
        >::result;
        return std::declval<outputs>();
    }

    template <CNode Node>
    constexpr static decltype(auto) Children()
    {
        using children = typename for_all<
            child_for_edge<Node>::template callable,
            std::tuple<>,
            edges
        >::result;
        return std::declval<children>();
    }

    template <CVisitor Visitor>
    constexpr static decltype(auto) DFS(Visitor&& visitor)
    {
        using inputs = std::remove_reference_t<decltype(Inputs())>;
        using ret = decltype(dfs_helper<NodeSet<>, Visitor, inputs>());
        return std::declval<ret>();
    }

    constexpr static bool ContainsCycle()
    {
        constexpr auto lambda = []<CNode Node>() constexpr{ return Empty{}; };
        using result = decltype(dfs_helper<NodeSet<>, decltype(lambda), Tasks...>());
        return contains_type_v<CycleDetected, std::remove_reference_t<result>>;
    }
};

/*****************************************************************************/
/* TASK GRAPH                                                                */
/*****************************************************************************/

export
template <std::derived_from<TaskBase>... Tasks>
requires (std::is_same_v<typename task_traits<Tasks>::return_type, PhaseCompletion> && ...)
      && (pe::is_unique_v<Tasks...>)
      && (not DAG<Tasks...>::ContainsCycle())
class TaskGraph
{
private:

    std::vector<pe::shared_ptr<TaskBase>> m_tasks;
    std::vector<pe::shared_ptr<TaskNode>> m_nodes;
    std::vector<Barrier>                  m_barriers;

public:

    template <typename... CreateInfos>
    requires (is_template_instance_v<CreateInfos, TaskCreateInfo> && ...)
          && (sizeof...(CreateInfos) == sizeof...(Tasks))
          && (pe::is_unique_v<typename CreateInfos::task_type...>)
          && (pe::contains_v<typename CreateInfos::task_type, Tasks> && ...)
    TaskGraph(CreateInfos... infos)
        : m_tasks{}
        , m_nodes{}
        , m_barriers{}
    {}

    void RunPhase()
    {}

    void Exit()
    {}
};

template <typename... CreateInfos>
TaskGraph(CreateInfos... infos) -> TaskGraph<typename CreateInfos::task_type...>;

} // namespace pe

