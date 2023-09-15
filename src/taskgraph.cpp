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

template <typename TEdge>
concept CEdge = pe::is_template_instance_v<TEdge, Edge>;

template <CEdge... Edges>
struct EdgeStack
{
    using type = std::tuple<Edges...>;

    constexpr static auto AfterPop()
    {
        return EdgeStack<>{};
    }

    template <CEdge Edge>
    constexpr static auto AfterPush()
    {
        return EdgeStack<>{};
    }
};

template <std::derived_from<TaskBase>... Tasks>
struct NodeSet
{
    using type = std::tuple<Tasks...>;

    template <std::derived_from<TaskBase> Task>
    constexpr static bool Contains()
    {
        return false;
    }
};

template <std::derived_from<TaskBase> A, std::derived_from<TaskBase> B>
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

template <std::derived_from<TaskBase>... Nodes>
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

template <std::derived_from<TaskBase>... Tasks>
struct DAG
{
    using nodes = std::tuple<Tasks...>;
    using edges = typename EdgeList<Tasks...>::edges;

    constexpr static auto Inputs()
    {
        return std::make_tuple();
    }

    constexpr static auto Outputs()
    {
        return std::make_tuple();
    }
};

/*****************************************************************************/
/* TASK GRAPH                                                                */
/*****************************************************************************/

export
template <std::derived_from<TaskBase>... Tasks>
requires (std::is_same_v<typename task_traits<Tasks>::return_type, PhaseCompletion> && ...)
      && (pe::is_unique_v<Tasks...>)
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

