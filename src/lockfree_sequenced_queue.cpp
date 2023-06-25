export module lockfree_sequenced_queue;

import platform;
import concurrency;
import lockfree_list;
import atomic_work;
import assert;
import shared_ptr;
import meta;
import logger;

import <atomic>;
import <type_traits>;
import <optional>;
import <any>;
import <memory>;
import <ranges>;

namespace pe{

/* A data structure with a queue-like API where the nodes
 * are actually stored in a set, hashed by a unique sequence
 * number. Using this framework it is possible to support a 
 * number of interesting operations such as 'ConditionallyEnqueue' 
 * in a lock-free manner.
 */
export
template <typename T>
class LockfreeSequencedQueue
{
public:

    enum class ProcessingResult
    {
        eIgnore,
        eDelete,
        eCycleBack,
        eNotFound,
    };

private:

    struct NodeProcessRequest
    {
        std::any           m_func;
        void              *m_shared_state;
        ProcessingResult (*m_processor)(std::any, void*, uint64_t, T);
    };

    struct NodeResultCommitRequest
    {
        T                m_node;
        uint64_t         m_seqnum;
        ProcessingResult m_result;
    };

    struct NodeProcessingResult
    {
        std::optional<T> m_node;
        ProcessingResult m_result;
        uint64_t         m_seqnum;
    };

    using HeadProcessingPipeline = AtomicWorkPipeline<
        LockfreeSequencedQueue<T>,
        /* Stage 1: Read and process the head */
        AtomicParallelWork<NodeProcessRequest, NodeResultCommitRequest, LockfreeSequencedQueue<T>>,
        /* Stage 2: Apply the result of the processing to the nodes list */
        AtomicParallelWork<NodeResultCommitRequest, NodeProcessingResult, LockfreeSequencedQueue<T>>
    >;

    struct ProcessHeadRequest
    {
        std::unique_ptr<HeadProcessingPipeline>                     m_pipeline;
        pe::shared_ptr<void>                                        m_state_ptr;
        pe::shared_ptr<pe::atomic_shared_ptr<NodeProcessingResult>> m_out;

        ProcessHeadRequest(std::unique_ptr<HeadProcessingPipeline>&& pipeline, 
            pe::shared_ptr<void> state_ptr, decltype(m_out) out)
            : m_pipeline{std::move(pipeline)}
            , m_state_ptr{state_ptr}
            , m_out{out}
        {}
    };

    struct ConditionalEnqueueRequest
    {
        std::any                         m_func;
        pe::shared_ptr<void>             m_state_ptr;
        void                            *m_shared_state;
        bool                           (*m_predicate)(std::any, void*, uint64_t, T);
        T                                m_node;
        LockfreeSet<T>&                  m_nodes;
        pe::shared_ptr<std::atomic_bool> m_out;

        template <typename Func>
        ConditionalEnqueueRequest(Func func, pe::shared_ptr<void> state_ptr,
            void *shared_state, decltype(m_predicate) predicate,
            T node, LockfreeSet<T>& nodes, pe::shared_ptr<std::atomic_bool> out)
            : m_func{func}
            , m_state_ptr{state_ptr}
            , m_shared_state{shared_state}
            , m_predicate{predicate}
            , m_node{node}
            , m_nodes{nodes}
            , m_out{out}
        {}
    };

    struct Request
    {
        enum class Type
        {
            eConditionalEnqueue,
            eProcessHead
        };

        using arg_type = std::variant<ConditionalEnqueueRequest, ProcessHeadRequest>;

        Type     m_type;
        arg_type m_arg;

        template <typename RequestType, typename... Args>
        Request(Type type, std::in_place_type_t<RequestType> reqtype, Args&&... args)
            : m_type{type}
            , m_arg{reqtype, std::forward<Args>(args)...}
        {}
    };

    AtomicStatefulSerialWork<Request> m_work;
    LockfreeSet<T>                    m_nodes;
    std::atomic_uint64_t              m_max_dequeued_seqnum;

    static void process_request(Request *request, uint64_t seqnum)
    {
        switch(request->m_type) {
        case Request::Type::eConditionalEnqueue: {
            const auto& arg = std::get<ConditionalEnqueueRequest>(request->m_arg);
            if(arg.m_predicate(arg.m_func, arg.m_shared_state, seqnum, arg.m_node)) {

                /* Note that this insertion operation can take place
                 * on lagging threads even after the ConditionalEnqueue
                 * request is considered processed. Hence, we are able
                 * to see "ghost insertions" at some point in the future.
                 * If the inserted node has not been dequeued, then it is
                 * not a hazard. If it has already been dequeued, then
                 * it is guaranteed that the 'max_dequeued_seqnum' value
                 * is equal to or greater than the ghost node's sequence
                 * number, and thus can be used to discard it.
                 */
                arg.m_nodes.Insert(seqnum, arg.m_node);
                arg.m_out->store(true, std::memory_order_release);
            }
            break;
        }
        case Request::Type::eProcessHead: {
            const auto& arg = std::get<ProcessHeadRequest>(request->m_arg);
            auto result = arg.m_pipeline->GetResult(seqnum);
            if(std::ranges::size(result) > 0) {
                auto ptr = pe::make_shared<NodeProcessingResult>(*std::ranges::begin(result));
                arg.m_out->store(ptr, std::memory_order_release);
            }
            break;
        }}
    }

public:

    template <typename RestartableFunc, typename SharedState>
    requires requires (RestartableFunc func, SharedState& state, uint64_t seqnum, T value) {
        {func(state, seqnum, value)} -> std::same_as<bool>;
    }
    bool ConditionallyEnqueue(RestartableFunc pred, SharedState& state, T value)
    {
        auto wrapped = +[](std::any func, void *state, uint64_t seqnum, T value) {
            SharedState& shared_state = *reinterpret_cast<SharedState*>(state);
            return any_cast<RestartableFunc>(func)(shared_state, seqnum, value);
        };
        /* The state_ptr is used to defer destruction of any shared state
         * that is passed in by a shared_ptr until the request is completed
         * and destroyed.
         */
        pe::shared_ptr<void> state_ptr{nullptr};
        if constexpr (is_template_instance<std::remove_cvref_t<SharedState>, pe::shared_ptr>{}) {
            state_ptr = state;
        }
        auto result = pe::make_shared<std::atomic_bool>(false);
        auto request = std::make_unique<Request>(
            Request::Type::eConditionalEnqueue,
            std::in_place_type_t<ConditionalEnqueueRequest>{},
            pred, state_ptr, &state, wrapped, value, m_nodes, result);

        m_work.PerformSerially(std::move(request), process_request);
        return result->load(std::memory_order_relaxed);
    }

    void Enqueue(T value)
    {
        ConditionallyEnqueue(+[](LockfreeSequencedQueue&, uint64_t, T){
            return true;
        }, *this, value);
    }

    template <typename RestartableFunc, typename SharedState>
    requires requires (RestartableFunc func, SharedState& state, uint64_t seqnum, T value) {
        {func(state, seqnum, value)} -> std::same_as<bool>;
    }
    std::pair<std::optional<T>, uint64_t> ConditionallyDequeue(RestartableFunc pred, SharedState& state)
    {
        auto ret = ProcessHead([pred](SharedState& state, uint64_t seqnum, T value){
            if(pred(state, seqnum, value))
                return ProcessingResult::eDelete;
            return ProcessingResult::eIgnore;
        }, state);
        uint64_t seqnum = std::get<2>(ret);
        if(std::get<1>(ret) == ProcessingResult::eDelete)
            return {std::get<0>(ret), seqnum};
        return {std::nullopt, seqnum};
    }

    std::optional<T> Dequeue()
    {
        auto ret = ProcessHead([](decltype(*this)& self, uint64_t seqnum, T value){
            return ProcessingResult::eDelete; 
        }, *this);
        if(std::get<1>(ret) == ProcessingResult::eDelete)
            return std::get<0>(ret);
        return std::nullopt;
    }

    template <typename RestartableFunc, typename SharedState>
    requires requires (RestartableFunc func, SharedState& state, uint64_t seqnum, T value) {
        {func(state, seqnum, value)} -> std::same_as<ProcessingResult>;
    }
    std::tuple<std::optional<T>, ProcessingResult, uint64_t> 
    ProcessHead(RestartableFunc processor, SharedState& shared_state)
    {
        auto wrapped = +[](std::any func, void *state, uint64_t seqnum, T value) {
            SharedState& shared_state = *reinterpret_cast<SharedState*>(state);
            return any_cast<RestartableFunc>(func)(shared_state, seqnum, value);
        };
        auto pipeline = std::make_unique<HeadProcessingPipeline>(
            std::views::single(NodeProcessRequest{processor, &shared_state, wrapped}), *this,
            +[](uint64_t seqnum, const NodeProcessRequest& req, LockfreeSequencedQueue& self){

                uint64_t max_dequeued_seqnum = 
                    self.m_max_dequeued_seqnum.load(std::memory_order_acquire);
                uint64_t node_seqnum;
                T value;
                do{
                    auto head = self.m_nodes.PeekHead();
                    if(!head.has_value()) {
                        return std::optional<NodeResultCommitRequest>{};
                    }
                    std::tie(node_seqnum, value) = head.value();
                    if(node_seqnum <= max_dequeued_seqnum) {
                        self.m_nodes.Delete(node_seqnum);
                    }
                }while(node_seqnum <= max_dequeued_seqnum);

                auto result = req.m_processor(req.m_func, req.m_shared_state, seqnum, value);
                return std::optional<NodeResultCommitRequest>{{value, node_seqnum, result}};
            },
            +[](uint64_t seqnum, const NodeResultCommitRequest& req, LockfreeSequencedQueue& self){

                switch(req.m_result) {
                case ProcessingResult::eCycleBack:
                    self.m_nodes.Insert(seqnum, req.m_node);
                    [[fallthrough]];
                case ProcessingResult::eDelete: {

                    /* Update the maximum dequeued sequence number to guarantee
                     * that this node will never be dequeued more than once.
                     */
                    uint64_t old_max_dequeued_seqnum = 
                        self.m_max_dequeued_seqnum.load(std::memory_order_acquire);
                    while(!self.m_max_dequeued_seqnum.compare_exchange_strong(
                        old_max_dequeued_seqnum, std::max(old_max_dequeued_seqnum, req.m_seqnum), 
                        std::memory_order_release, std::memory_order_acquire));
                    break;
                }
                case ProcessingResult::eIgnore:
                    /* no-op */
                    break;
                case ProcessingResult::eNotFound:
                    pe::assert(0);
                    break;
                }
                return std::optional<NodeProcessingResult>{{{req.m_node}, req.m_result, seqnum}};
            }
        );
        /* Retain the result location until no more threads are touching it,
         * and allow setting it atomically.
         */
        auto result = pe::make_shared<pe::atomic_shared_ptr<NodeProcessingResult>>();

        pe::shared_ptr<void> state_ptr{nullptr};
        if constexpr (is_template_instance<std::remove_cvref_t<SharedState>, pe::shared_ptr>{}) {
            state_ptr = shared_state;
        }

        auto request = std::make_unique<Request>(
            Request::Type::eProcessHead, 
            std::in_place_type_t<ProcessHeadRequest>{},
            std::move(pipeline), state_ptr, result);

        m_work.PerformSerially(std::move(request), process_request);

        std::optional<T> ret{};
        ProcessingResult presult = ProcessingResult::eNotFound;
        uint64_t seqnum = 0;
        if(auto ptr = result->load(std::memory_order_acquire)) {
            ret = ptr->m_node;
            presult = ptr->m_result;
            seqnum = ptr->m_seqnum;
        }
        return {ret, presult, seqnum};
    }
};

} // namespace pe

