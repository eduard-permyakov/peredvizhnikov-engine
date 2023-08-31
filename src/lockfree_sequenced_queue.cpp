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
        std::any              m_processor_func;
        std::any              m_fallback_func;
        pe::shared_ptr<void>  m_shared_state;
        ProcessingResult    (*m_processor)(std::any, pe::shared_ptr<void>, uint64_t, T);
        void                (*m_fallback)(std::any, pe::shared_ptr<void>, uint64_t);
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
        bool                           (*m_predicate)(std::any, pe::shared_ptr<void>, uint64_t, T);
        T                                m_node;
        LockfreeSet<T>&                  m_nodes;
        pe::shared_ptr<std::atomic_bool> m_out;

        template <typename Func>
        ConditionalEnqueueRequest(Func func, pe::shared_ptr<void> shared_state, 
            decltype(m_predicate) predicate, T node, LockfreeSet<T>& nodes, 
            pe::shared_ptr<std::atomic_bool> out)
            : m_func{func}
            , m_state_ptr{shared_state}
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

    struct alignas(8) DequeueState
    {
        /* The sequnce number of the last request to 
         * update the max_dequeued seqnum.
         */
        uint32_t m_last_dequeue_req_seqnum;
        /* The sequence number of the last dequeued node.
         */
        uint32_t m_max_dequeued_node_seqnum;
    };

    using AtomicDequeueState = std::atomic<DequeueState>;
    static_assert(sizeof(AtomicDequeueState) == sizeof(uint64_t));
    static_assert(AtomicDequeueState::is_always_lock_free);

    AtomicStatefulSerialWork<Request> m_work;
    LockfreeSet<T>                    m_nodes;
    AtomicDequeueState                m_dequeue_state;

    static inline auto s_consumed_marker = pe::make_shared<NodeProcessingResult>();

    static void process_request(Request *request, uint64_t seqnum)
    {
        switch(request->m_type) {
        case Request::Type::eConditionalEnqueue: {
            const auto& arg = std::get<ConditionalEnqueueRequest>(request->m_arg);
            if(arg.m_predicate(arg.m_func, arg.m_state_ptr, seqnum, arg.m_node)) {

                /* Note that this insertion operation can take place
                 * on lagging threads even after the ConditionalEnqueue
                 * request is considered processed. Hence, we are able
                 * to see "ghost insertions" at some point in the future.
                 * If the inserted node has not been dequeued, then it is
                 * not a hazard. If it has already been dequeued, then
                 * it is guaranteed that the 'max_dequeued_node_seqnum' 
                 * value is equal to or greater than the ghost node's 
                 * sequence number, and thus can be used to discard it.
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
                auto curr = arg.m_out->load(std::memory_order_relaxed);
                while(!curr) {
                    arg.m_out->compare_exchange_strong(curr, ptr, 
                        std::memory_order_release, std::memory_order_relaxed);
                }
            }
            break;
        }}
    }

public:

    template <typename RestartableFunc, typename SharedState>
    requires requires (RestartableFunc func, pe::shared_ptr<SharedState> state, uint64_t seqnum, T value) {
        {func(state, seqnum, value)} -> std::same_as<bool>;
    }
    bool ConditionallyEnqueue(RestartableFunc pred, pe::shared_ptr<SharedState> state, T value,
        std::optional<uint32_t> seqnum = std::nullopt)
    {
        auto wrapped = +[](std::any func, pe::shared_ptr<void> state, uint64_t seqnum, T value) {
            auto shared_state = pe::static_pointer_cast<SharedState>(state);
            return any_cast<RestartableFunc>(func)(shared_state, seqnum, value);
        };
        auto result = pe::make_shared<std::atomic_bool>(false);
        auto request = std::make_unique<Request>(
            Request::Type::eConditionalEnqueue,
            std::in_place_type_t<ConditionalEnqueueRequest>{},
            pred, state, wrapped, value, m_nodes, result);

        m_work.PerformSerially(std::move(request), process_request, seqnum);
        return result->load(std::memory_order_relaxed);
    }

    void Enqueue(T value, std::optional<uint32_t> seqnum = std::nullopt)
    {
        ConditionallyEnqueue(+[](LockfreeSequencedQueue&, uint64_t, T){
            return true;
        }, *this, value, seqnum);
    }

    template <typename RestartableProcessorFunc, typename SharedState>
    requires requires (RestartableProcessorFunc func, pe::shared_ptr<SharedState> state, 
        uint64_t seqnum, T value) {

        {func(state, seqnum, value)} -> std::same_as<bool>;
    }
    std::pair<std::optional<T>, uint64_t> ConditionallyDequeue(RestartableProcessorFunc pred, 
        pe::shared_ptr<SharedState> state)
    {
        auto ret = ProcessHead([pred](pe::shared_ptr<SharedState> state, uint64_t seqnum, T value){
            if(pred(state, seqnum, value))
                return ProcessingResult::eDelete;
            return ProcessingResult::eIgnore;
        }, [](pe::shared_ptr<SharedState> state, uint64_t seqnum){}, state);
        uint64_t seqnum = std::get<2>(ret);
        if(std::get<1>(ret) == ProcessingResult::eDelete)
            return {std::get<0>(ret), seqnum};
        return {std::nullopt, seqnum};
    }

    std::optional<T> Dequeue(std::optional<uint32_t> seqnum = std::nullopt)
    {
        auto state = pe::make_shared<std::monostate>();
        auto ret = ProcessHead([](pe::shared_ptr<std::monostate>, uint64_t seqnum, T value){
            return ProcessingResult::eDelete; 
        }, [](decltype(state), uint64_t){}, state, seqnum);
        if(std::get<1>(ret) == ProcessingResult::eDelete)
            return std::get<0>(ret);
        return std::nullopt;
    }

    template <typename RestartableProcessorFunc, typename RestartableFallbackFunc, typename SharedState>
    requires requires (RestartableProcessorFunc processor, RestartableFallbackFunc fallback, 
        pe::shared_ptr<SharedState> state, uint64_t seqnum, T value) {

        {processor(state, seqnum, value)} -> std::same_as<ProcessingResult>;
        {fallback(state, seqnum)} -> std::same_as<void>;
    }
    std::tuple<std::optional<T>, ProcessingResult, uint64_t> 
    ProcessHead(RestartableProcessorFunc processor, RestartableFallbackFunc fallback, 
        pe::shared_ptr<SharedState> shared_state, std::optional<uint32_t> seqnum = std::nullopt)
    {
        auto wrapped_processor = +[](std::any func, pe::shared_ptr<void> state, uint64_t seqnum, T value) {
            auto shared_state = pe::static_pointer_cast<SharedState>(state);
            return any_cast<RestartableProcessorFunc>(func)(shared_state, seqnum, value);
        };
        auto wrapped_fallback = +[](std::any func, pe::shared_ptr<void> state, uint64_t seqnum) {
            auto shared_state = pe::static_pointer_cast<SharedState>(state);
            auto callable = any_cast<RestartableFallbackFunc>(func);
            callable(shared_state, seqnum);
        };
        NodeProcessRequest node_request{processor, fallback, 
            pe::static_pointer_cast<void>(shared_state),
            wrapped_processor, wrapped_fallback};

        auto pipeline = std::make_unique<HeadProcessingPipeline>(
            std::views::single(node_request), *this,
            +[](uint64_t seqnum, const NodeProcessRequest& req, LockfreeSequencedQueue& self){

                DequeueState dequeue_state = 
                    self.m_dequeue_state.load(std::memory_order_acquire);

                /* It's a 'lagging' request 
                 */
                if(dequeue_state.m_last_dequeue_req_seqnum > seqnum) {
                    req.m_fallback(req.m_fallback_func, req.m_shared_state, seqnum);
                    return std::optional<NodeResultCommitRequest>{};
                }

                /* A competing thread executing the same request has 
                 * already selected a node for deletion.
                 */
                if(dequeue_state.m_last_dequeue_req_seqnum == seqnum) {
                    uint32_t node_seqnum = dequeue_state.m_max_dequeued_node_seqnum;
                    auto node = self.m_nodes.Get(node_seqnum);
                    if(node.has_value()) {
                        auto value = node.value();
                        auto result = req.m_processor(req.m_processor_func, req.m_shared_state, seqnum, value);
                        return std::optional<NodeResultCommitRequest>{{value, node_seqnum, result}};
                    }
                    req.m_fallback(req.m_fallback_func, req.m_shared_state, seqnum);
                    return std::optional<NodeResultCommitRequest>{};
                }

                uint64_t node_seqnum;
                T value;
                do{
                    auto head = self.m_nodes.PeekHead();
                    if(!head.has_value()) {
                        req.m_fallback(req.m_fallback_func, req.m_shared_state, seqnum);
                        return std::optional<NodeResultCommitRequest>{};
                    }
                    std::tie(node_seqnum, value) = head.value();
                    if(node_seqnum <= dequeue_state.m_max_dequeued_node_seqnum) {
                        self.m_nodes.Delete(node_seqnum);
                    }
                }while(node_seqnum <= dequeue_state.m_max_dequeued_node_seqnum);

                auto result = req.m_processor(req.m_processor_func, req.m_shared_state, seqnum, value);
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
                    DequeueState old_dequeue_state = 
                        self.m_dequeue_state.load(std::memory_order_acquire);
                    uint32_t new_max_dequeued_node_seqnum;
                    do{
                        /* This is a 'lagging' request, the node
                         * has already been dequeued.
                         */
                        if(old_dequeue_state.m_last_dequeue_req_seqnum > seqnum)
                            return std::optional<NodeProcessingResult>{};

                        /* A different thread servicing the same request
                         * has already updated the dequeue state.
                         */
                        if(old_dequeue_state.m_last_dequeue_req_seqnum == seqnum) {
                            return std::optional<NodeProcessingResult>{
                                {{req.m_node}, req.m_result, seqnum}};
                        }

                        new_max_dequeued_node_seqnum = std::max(
                            old_dequeue_state.m_max_dequeued_node_seqnum,
                            static_cast<uint32_t>(req.m_seqnum));

                    }while(!self.m_dequeue_state.compare_exchange_strong(old_dequeue_state, 
                        {static_cast<uint32_t>(seqnum), new_max_dequeued_node_seqnum},
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
        auto request = std::make_unique<Request>(
            Request::Type::eProcessHead, 
            std::in_place_type_t<ProcessHeadRequest>{},
            std::move(pipeline), shared_state, result);

        m_work.PerformSerially(std::move(request), process_request, seqnum);

        std::optional<T> ret{};
        ProcessingResult presult = ProcessingResult::eNotFound;
        uint64_t seq = 0;
        if(auto ptr = result->load(std::memory_order_acquire)) {
            ret = ptr->m_node;
            presult = ptr->m_result;
            seq = ptr->m_seqnum;
        }
        result->store(s_consumed_marker, std::memory_order_release);
        return {ret, presult, seq};
    }
};

} // namespace pe

